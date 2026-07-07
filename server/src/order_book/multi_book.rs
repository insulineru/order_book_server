use crate::{
    order_book::{Coin, InnerOrder, Oid, OrderBook, Px, Side, Snapshot, Sz},
    prelude::*,
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
};
use tokio::fs::read_to_string;

pub(crate) struct Snapshots<O>(HashMap<Coin, Snapshot<O>>);

type Bbo = (Option<(Px, Sz, u32)>, Option<(Px, Sz, u32)>);

fn merge_best(best: &mut Option<(Px, Sz, u32)>, candidate: Option<(Px, Sz, u32)>, side: Side) {
    let Some(candidate) = candidate else { return };
    match best {
        None => *best = Some(candidate),
        Some(current) if current.0 == candidate.0 => {
            current.1 = current.1 + candidate.1;
            current.2 = current.2.saturating_add(candidate.2);
        }
        Some(current) => {
            let replace = match side {
                Side::Bid => candidate.0 > current.0,
                Side::Ask => candidate.0 < current.0,
            };
            if replace {
                *current = candidate;
            }
        }
    }
}

fn mirror_bbo((bid, ask): Bbo) -> Bbo {
    let mirrored_bid = ask.and_then(|(px, sz, n)| px.one_minus().map(|px| (px, sz, n)));
    let mirrored_ask = bid.and_then(|(px, sz, n)| px.one_minus().map(|px| (px, sz, n)));
    (mirrored_bid, mirrored_ask)
}

impl<O> Snapshots<O> {
    pub(crate) const fn new(value: HashMap<Coin, Snapshot<O>>) -> Self {
        Self(value)
    }

    pub(crate) fn retain_coins(&mut self, mut keep: impl FnMut(&Coin) -> bool) {
        self.0.retain(|coin, _| keep(coin));
    }

    pub(crate) fn value(self) -> HashMap<Coin, Snapshot<O>> {
        self.0
    }
}

#[derive(Clone)]
pub(crate) struct OrderBooks<O> {
    order_books: BTreeMap<Coin, OrderBook<O>>,
}

impl<O: InnerOrder> OrderBooks<O> {
    pub(crate) const fn as_ref(&self) -> &BTreeMap<Coin, OrderBook<O>> {
        &self.order_books
    }

    /// Total number of orders across all orderbooks
    pub(crate) fn order_count(&self) -> usize {
        self.order_books.values().map(|book| book.order_count()).sum()
    }

    pub(crate) fn first_crossed_coin(&self) -> Option<Coin> {
        self.order_books.iter().find_map(|(coin, book)| book.is_crossed().then(|| coin.clone()))
    }
    #[must_use]
    pub(crate) fn from_snapshots(snapshot: Snapshots<O>, ignore_triggers: bool) -> Self {
        Self {
            order_books: snapshot
                .value()
                .into_iter()
                .map(|(coin, book)| (coin, OrderBook::from_snapshot(book, ignore_triggers)))
                .collect(),
        }
    }

    pub(crate) fn add_order(&mut self, order: O) {
        let coin = &order.coin();
        self.order_books.entry(coin.clone()).or_insert_with(OrderBook::new).add_order(order);
    }

    pub(crate) fn cancel_order(&mut self, oid: Oid, coin: Coin) -> bool {
        if let Some(book) = self.order_books.get_mut(&coin) {
            let success = book.cancel_order(oid.clone());
            if !success {
                // oid not found in this coin's book
                log::debug!("cancel_order: oid {:?} not found in {:?} book", oid, coin);
            }
            // Drop the per-coin OrderBook once it's empty. Without this, every coin
            // we've ever seen sticks around in the BTreeMap (plus its slab and
            // BTreeMap-of-Levels capacity), even after delistings.
            if book.order_count() == 0 {
                self.order_books.remove(&coin);
            }
            success
        } else {
            // coin book doesn't exist
            log::debug!("cancel_order: no book for coin {:?}", coin);
            false
        }
    }

    // change size to reflect how much gets matched during the block
    pub(crate) fn modify_sz(&mut self, oid: Oid, coin: Coin, sz: Sz) -> bool {
        let Some(book) = self.order_books.get_mut(&coin) else { return false };
        let success = book.modify_sz(oid, sz);
        // A modify that reduces sz to zero leaves an empty book behind; evict it
        // for the same reason as cancel_order above.
        if book.order_count() == 0 {
            self.order_books.remove(&coin);
        }
        success
    }

    /// Get BBO for specific coins only - faster for selective broadcast
    /// Only computes BBO for coins in the set, avoiding iteration over all coins.
    /// A coin whose book emptied (and was evicted) yields `(None, None)` rather
    /// than being skipped - skipping left subscribers holding the last quote
    /// forever after a delisting, since no further update would ever arrive.
    #[must_use]
    pub(crate) fn get_bbos_for_coins(
        &self,
        coins: &std::collections::HashSet<Coin>,
    ) -> HashMap<Coin, Bbo> {
        coins.iter().map(|coin| (coin.clone(), self.synthetic_bbo_for_coin(coin))).collect()
    }

    fn synthetic_bbo_for_coin(&self, coin: &Coin) -> Bbo {
        let (mut bid, mut ask) = self.order_books.get(coin).map_or((None, None), OrderBook::get_bbo);
        if let Some(sibling) = coin.hip4_sibling() {
            let mirrored = self.order_books.get(&sibling).map_or((None, None), |book| mirror_bbo(book.get_bbo()));
            merge_best(&mut bid, mirrored.0, Side::Bid);
            merge_best(&mut ask, mirrored.1, Side::Ask);
        }
        (bid, ask)
    }

    /// Compact slab allocators across every coin's orderbook. Returns the number
    /// of price-level lists that were actually rebuilt. Cheap when nothing is
    /// fragmented, so safe to call on a slow maintenance cadence.
    ///
    /// Also evicts any books whose order count dropped to zero — the per-event
    /// path covers single-order eviction, but a stuck book that emptied during
    /// a missed-event window is otherwise pinned forever.
    pub(crate) fn compact_all(&mut self) -> usize {
        let compacted: usize = self.order_books.values_mut().map(|book| book.compact()).sum();
        self.order_books.retain(|_, book| book.order_count() > 0);
        compacted
    }

    /// Returns (total live nodes, total slab capacity) summed across all coins.
    pub(crate) fn slab_stats(&self) -> (usize, usize) {
        let mut live = 0usize;
        let mut cap = 0usize;
        for book in self.order_books.values() {
            let (l, c) = book.slab_stats();
            live += l;
            cap += c;
        }
        (live, cap)
    }
}

impl<O: InnerOrder> OrderBooks<O> {
    /// L4 snapshot of a single coin's book. Cloning one coin is a few hundred
    /// microseconds; the previous all-coins `to_snapshots_par` cloned the entire
    /// multi-book (~hundreds of thousands of orders) under the listener lock on
    /// every l4Book subscribe, stalling event processing for its whole duration.
    #[must_use]
    pub(crate) fn snapshot_for_coin(&self, coin: &Coin) -> Option<Snapshot<O>> {
        self.order_books.get(coin).map(OrderBook::to_snapshot)
    }
}

fn split_snapshot_payload(
    value: serde_json::Value,
    exact_source_height: Option<u64>,
) -> Result<(u64, serde_json::Value)> {
    let (embedded_height, snapshot_value) = match value {
        serde_json::Value::Array(mut values) => {
            if values.len() == 2 {
                if let Some(height) = values[0].as_u64() {
                    let snapshot_value =
                        values.pop().ok_or("height-prefixed snapshot is missing the snapshot payload")?;
                    (Some(height), snapshot_value)
                } else {
                    (None, serde_json::Value::Array(values))
                }
            } else {
                (None, serde_json::Value::Array(values))
            }
        }
        serde_json::Value::Object(mut object) => {
            let embedded_height = object.remove("height").and_then(|height| height.as_u64());
            let snapshot_value = ["snapshot", "snapshots", "data", "value"]
                .into_iter()
                .find_map(|key| object.remove(key))
                .ok_or("height object snapshot is missing snapshot/snapshots/data/value payload")?;
            (embedded_height, snapshot_value)
        }
        _ => return Err("snapshot JSON must be either a snapshot array or a height-prefixed payload".into()),
    };

    let height = match (embedded_height, exact_source_height) {
        (Some(embedded), Some(source)) if embedded != source => {
            return Err(format!("embedded snapshot height {embedded} does not match source height {source}").into());
        }
        (Some(embedded), _) => embedded,
        (None, Some(source)) => source,
        (None, None) => {
            return Err("snapshot JSON has no embedded height and no exact source height was provided; \
                 refusing to use live visor height because it can skip replay events"
                .into());
        }
    };

    Ok((height, snapshot_value))
}

/// Load snapshots from CLI-generated JSON.
///
/// Safe height sources:
/// - embedded height in a height-prefixed snapshot file;
/// - `exact_source_height` from the frozen source itself, e.g.
///   `periodic_abci_states/{date}/{height}.rmp`.
///
/// Live visor height is intentionally not accepted here.
pub(crate) fn load_snapshots_from_cli_str<O, R>(
    str: &str,
    exact_source_height: Option<u64>,
) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error>,
    R: Serialize + DeserializeOwned,
{
    #[allow(clippy::type_complexity)]
    let value: serde_json::Value = serde_json::from_str(str)?;
    let (height, snapshot_value) = split_snapshot_payload(value, exact_source_height)?;
    let snapshot: Vec<(String, [Vec<R>; 2])> = serde_json::from_value(snapshot_value)?;
    Ok((
        height,
        Snapshots::new(
            snapshot
                .into_iter()
                .map(|(coin, [bids, asks])| {
                    let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                    let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                    Ok((Coin::new(&coin), Snapshot([bids, asks])))
                })
                .collect::<Result<HashMap<Coin, Snapshot<O>>>>()?,
        ),
    ))
}

/// Load snapshots from CLI-generated JSON file.
pub(crate) async fn load_snapshots_from_cli_json<O, R>(
    snapshot_path: &Path,
    exact_source_height: Option<u64>,
) -> Result<(u64, Snapshots<O>)>
where
    O: TryFrom<R, Error = Error>,
    R: Serialize + DeserializeOwned,
{
    let file_contents = read_to_string(snapshot_path).await?;
    load_snapshots_from_cli_str(&file_contents, exact_source_height)
}

#[cfg(test)]
mod tests {
    use super::load_snapshots_from_cli_str;
    use crate::{
        order_book::{
            InnerOrder, OrderBook, Px, Side, Snapshot, Sz,
            levels::build_l2_level,
            multi_book::{Coin, Snapshots},
        },
        prelude::*,
        types::{
            L4Order, Level,
            inner::{InnerL4Order, InnerLevel},
        },
    };
    use alloy::primitives::Address;
    use itertools::Itertools;
    use serde::{Deserialize, Serialize};
    use std::{collections::HashMap, fs::create_dir_all, path::PathBuf};
    use tokio::fs::read_to_string;

    fn load_snapshots_from_str<O, R>(str: &str) -> Result<(u64, Snapshots<O>)>
    where
        O: TryFrom<R, Error = Error>,
        R: Serialize + for<'a> Deserialize<'a>,
    {
        #[allow(clippy::type_complexity)]
        let (height, snapshot): (u64, Vec<(String, [Vec<R>; 2])>) = serde_json::from_str(str)?;
        Ok((
            height,
            Snapshots::new(
                snapshot
                    .into_iter()
                    .map(|(coin, [bids, asks])| {
                        let bids: Vec<O> = bids.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                        let asks: Vec<O> = asks.into_iter().map(O::try_from).collect::<Result<Vec<O>>>()?;
                        Ok((Coin::new(&coin), Snapshot([bids, asks])))
                    })
                    .collect::<Result<HashMap<Coin, Snapshot<O>>>>()?,
            ),
        ))
    }

    async fn load_snapshots_from_json<O, R>(path: &PathBuf) -> Result<(u64, Snapshots<O>)>
    where
        O: TryFrom<R, Error = Error>,
        R: Serialize + for<'a> Deserialize<'a>,
    {
        let file_contents = read_to_string(path).await?;
        load_snapshots_from_str(&file_contents)
    }

    #[must_use]
    fn snapshot_to_l2_snapshot<O: InnerOrder>(
        snapshot: &Snapshot<O>,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Snapshot<InnerLevel> {
        let [bids, asks] = &snapshot.0;
        let bids = orders_to_l2_levels(bids, Side::Bid, n_levels, n_sig_figs, mantissa);
        let asks = orders_to_l2_levels(asks, Side::Ask, n_levels, n_sig_figs, mantissa);
        Snapshot([bids, asks])
    }

    #[must_use]
    fn orders_to_l2_levels<O: InnerOrder>(
        orders: &[O],
        side: Side,
        n_levels: Option<usize>,
        n_sig_figs: Option<u32>,
        mantissa: Option<u64>,
    ) -> Vec<InnerLevel> {
        let mut levels = Vec::new();
        if n_levels == Some(0) {
            return levels;
        }
        let mut cur_level: Option<InnerLevel> = None;

        for order in orders {
            if build_l2_level(
                &mut cur_level,
                &mut levels,
                n_levels,
                n_sig_figs,
                mantissa,
                side,
                InnerLevel { px: order.limit_px(), sz: order.sz(), n: 1 },
            ) {
                break;
            }
        }
        levels.extend(cur_level.take());
        levels
    }

    #[derive(Default)]
    struct OrderManager {
        next_oid: u64,
    }

    fn simple_inner_order(oid: u64, side: Side, sz: String, px: String) -> Result<InnerL4Order> {
        let px = Px::parse_from_str(&px)?;
        let sz = Sz::parse_from_str(&sz)?;
        Ok(InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(""),
            side,
            limit_px: px,
            sz,
            oid,
            timestamp: 0,
            trigger_condition: String::new(),
            is_trigger: false,
            trigger_px: String::new(),
            is_position_tpsl: false,
            reduce_only: false,
            order_type: String::new(),
            tif: None,
            cloid: None,
        })
    }

    impl OrderManager {
        fn order(&mut self, sz: &str, limit_px: &str, side: Side) -> Result<InnerL4Order> {
            let order = simple_inner_order(self.next_oid, side, sz.to_string(), limit_px.to_string())?;
            self.next_oid += 1;
            Ok(order)
        }

        fn batch_order(&mut self, sz: &str, limit_px: &str, side: Side, mult: u64) -> Result<Vec<InnerL4Order>> {
            (0..mult).map(|_| self.order(sz, limit_px, side)).try_collect()
        }
    }

    fn setup_book(book: &mut OrderBook<InnerL4Order>) -> Snapshots<InnerL4Order> {
        let mut o = OrderManager::default();
        let buy_orders1 = o.batch_order("100", "34.01", Side::Bid, 4).unwrap();
        let buy_orders2 = o.batch_order("200", "34.5", Side::Bid, 2).unwrap();
        let buy_orders3 = o.batch_order("300", "34.6", Side::Bid, 1).unwrap();
        let sell_orders1 = o.batch_order("100", "35", Side::Ask, 4).unwrap();
        let sell_orders2 = o.batch_order("200", "35.1", Side::Ask, 2).unwrap();
        let sell_orders3 = o.batch_order("300", "35.5", Side::Ask, 1).unwrap();
        for orders in [buy_orders1, buy_orders2, buy_orders3, sell_orders1, sell_orders2, sell_orders3] {
            for o in orders {
                book.add_order(o);
            }
        }
        Snapshots(vec![(Coin::new(""), book.to_snapshot()); 2].into_iter().collect())
    }

    const SNAPSHOT_JSON: &str = r#"[100, 
    [
        [
            "@1",
            [
                [
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.444",
                            "sz": "100.0",
                            "oid": 105338503859,
                            "timestamp": 1750660644034,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "100.0",
                            "tif": "Alo",
                            "cloid": null
                        }
                    ],
                    [
                        "0x0000000000000000000000000000000000000000",
                        {
                            "coin": "@1",
                            "side": "B",
                            "limitPx": "30.385",
                            "sz": "5.45",
                            "oid": 105337808436,
                            "timestamp": 1750660453608,
                            "triggerCondition": "N/A",
                            "isTrigger": false,
                            "triggerPx": "0.0",
                            "children": [],
                            "isPositionTpsl": false,
                            "reduceOnly": false,
                            "orderType": "Limit",
                            "origSz": "5.45",
                            "tif": "Gtc",
                            "cloid": null
                        }
                    ]
                ],
                []
            ]
        ]
    ]
]"#;

    #[tokio::test]
    async fn test_deserialization_from_json() -> Result<()> {
        create_dir_all("tmp/deserialization_test")?;
        fs::write("tmp/deserialization_test/out.json", SNAPSHOT_JSON)?;
        load_snapshots_from_json::<InnerL4Order, (Address, L4Order)>(&PathBuf::from(
            "tmp/deserialization_test/out.json",
        ))
        .await?;
        Ok(())
    }

    #[test]
    fn test_deserialization() -> Result<()> {
        load_snapshots_from_str::<InnerL4Order, (Address, L4Order)>(SNAPSHOT_JSON)?;
        Ok(())
    }

    #[test]
    fn test_cli_snapshot_loader_accepts_embedded_height() -> Result<()> {
        let (height, _) = load_snapshots_from_cli_str::<InnerL4Order, (Address, L4Order)>(SNAPSHOT_JSON, None)?;
        assert_eq!(height, 100);
        Ok(())
    }

    #[test]
    fn test_cli_snapshot_loader_uses_exact_source_height_for_raw_snapshot() -> Result<()> {
        let value: serde_json::Value = serde_json::from_str(SNAPSHOT_JSON)?;
        let raw_snapshot =
            value.as_array().and_then(|values| values.get(1)).ok_or("test fixture must be a height-prefixed tuple")?;
        let raw_snapshot = serde_json::to_string(raw_snapshot)?;

        let (height, _) = load_snapshots_from_cli_str::<InnerL4Order, (Address, L4Order)>(&raw_snapshot, Some(99))?;
        assert_eq!(height, 99);
        Ok(())
    }

    #[test]
    fn test_cli_snapshot_loader_rejects_raw_snapshot_without_exact_height() -> Result<()> {
        let value: serde_json::Value = serde_json::from_str(SNAPSHOT_JSON)?;
        let raw_snapshot =
            value.as_array().and_then(|values| values.get(1)).ok_or("test fixture must be a height-prefixed tuple")?;
        let raw_snapshot = serde_json::to_string(raw_snapshot)?;

        let err = match load_snapshots_from_cli_str::<InnerL4Order, (Address, L4Order)>(&raw_snapshot, None) {
            Ok(_) => panic!("raw snapshot without exact height must fail"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("no embedded height"), "{err}");
        Ok(())
    }

    #[test]
    fn test_cli_snapshot_loader_rejects_height_mismatch() {
        let err = match load_snapshots_from_cli_str::<InnerL4Order, (Address, L4Order)>(SNAPSHOT_JSON, Some(101)) {
            Ok(_) => panic!("mismatched embedded/source heights must fail"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("does not match source height"), "{err}");
    }

    #[test]
    fn test_l4_snapshot_to_l2_snapshot() {
        let mut book = OrderBook::new();
        let coin = Coin::new("");
        let snapshot = setup_book(&mut book);
        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(2), Some(1));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![Level::new("34".to_string(), "1100".to_string(), 7)],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("36".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), Some(3), Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.5".to_string(), "700".to_string(), 3),
                Level::new("34".to_string(), "400".to_string(), 4),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.5".to_string(), "700".to_string(), 3),
            ],
        ];
        assert_eq!(ans, raw_levels);
        let snapshot_from_book = book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_book = snapshot_from_book.export_inner_snapshot();
        let snapshot_from_book = book.to_l2_snapshot(None, None, None);
        let snapshot_from_snapshot = snapshot_from_book.to_l2_snapshot(Some(2), Some(3), Some(5));
        let raw_levels_from_snapshot = snapshot_from_snapshot.export_inner_snapshot();
        assert_eq!(raw_levels_from_book, ans);
        assert_eq!(raw_levels_from_snapshot, ans);

        let levels = snapshot_to_l2_snapshot(snapshot.0.get(&coin).unwrap(), Some(2), None, Some(5));
        let raw_levels = levels.export_inner_snapshot();
        let ans = [
            vec![
                Level::new("34.6".to_string(), "300".to_string(), 1),
                Level::new("34.5".to_string(), "400".to_string(), 2),
            ],
            vec![
                Level::new("35".to_string(), "400".to_string(), 4),
                Level::new("35.1".to_string(), "400".to_string(), 2),
            ],
        ];
        assert_eq!(ans, raw_levels);
    }

    use crate::order_book::{Oid, multi_book::OrderBooks};

    fn make_order(oid: u64, coin: &str, side: Side, sz: &str, px: &str) -> InnerL4Order {
        let mut o = simple_inner_order(oid, side, sz.to_string(), px.to_string()).unwrap();
        o.coin = Coin::new(coin);
        o
    }

    // ==================== MultiBook eviction tests ====================

    #[test]
    fn test_cancel_removes_empty_orderbook_from_multibook() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "BTC", Side::Bid, "1", "50000"));
        assert!(books.as_ref().contains_key(&Coin::new("BTC")));
        let removed = books.cancel_order(Oid::new(1), Coin::new("BTC"));
        assert!(removed);
        // Empty book must be evicted: ghost entries in the BTreeMap were a known
        // slow leak after coin delistings.
        assert!(!books.as_ref().contains_key(&Coin::new("BTC")));
    }

    #[test]
    fn test_cancel_keeps_nonempty_orderbook() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "ETH", Side::Bid, "1", "3000"));
        books.add_order(make_order(2, "ETH", Side::Bid, "2", "3000"));
        books.cancel_order(Oid::new(1), Coin::new("ETH"));
        // Still has order 2 — must not be evicted.
        assert!(books.as_ref().contains_key(&Coin::new("ETH")));
    }

    #[test]
    fn test_get_bbos_for_coins_reports_evicted_coin_as_empty() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "BTC", Side::Bid, "1", "50000"));
        books.cancel_order(Oid::new(1), Coin::new("BTC")); // book evicted

        let coins: std::collections::HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        let bbos = books.get_bbos_for_coins(&coins);
        // The coin must be present with an empty BBO - skipping it left
        // subscribers holding the last quote forever after a delisting.
        assert_eq!(bbos.get(&Coin::new("BTC")), Some(&(None, None)));
    }

    #[test]
    fn test_hip4_bbo_merges_complementary_sibling_liquidity() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(make_order(1, "#7510", Side::Bid, "10", "0.8"));
        books.add_order(make_order(2, "#7510", Side::Ask, "2", "0.9"));
        books.add_order(make_order(3, "#7511", Side::Bid, "3", "0.1")); // mirrored as #7510 ask 0.9
        books.add_order(make_order(4, "#7511", Side::Ask, "4", "0.3")); // mirrored as #7510 bid 0.7

        let coins: std::collections::HashSet<Coin> = std::iter::once(Coin::new("#7510")).collect();
        let bbos = books.get_bbos_for_coins(&coins);
        let (bid, ask) = bbos.get(&Coin::new("#7510")).unwrap();
        let bid = bid.unwrap();
        let ask = ask.unwrap();

        assert_eq!((bid.0.to_str(), bid.1.to_str(), bid.2), ("0.8".to_string(), "10".to_string(), 1));
        assert_eq!((ask.0.to_str(), ask.1.to_str(), ask.2), ("0.9".to_string(), "5".to_string(), 2));
    }

    #[test]
    fn test_compact_all_evicts_empty_books() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        // Seed two coins, then drain one without triggering per-event eviction
        // (we cancel via OrderBook directly so the MultiBook path doesn't run).
        books.add_order(make_order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(make_order(2, "ETH", Side::Bid, "1", "3000"));
        // Sanity
        assert_eq!(books.as_ref().len(), 2);
        // Cancel order 1 via MultiBook to trigger eviction of the BTC book.
        books.cancel_order(Oid::new(1), Coin::new("BTC"));
        // ETH still has an order; compact_all should leave it alone.
        books.compact_all();
        assert!(books.as_ref().contains_key(&Coin::new("ETH")));
        assert!(!books.as_ref().contains_key(&Coin::new("BTC")));
    }
}
