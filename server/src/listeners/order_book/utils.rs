use crate::{
    listeners::order_book::{L2SnapshotParams, L2Snapshots},
    metrics::RESYNC_PHASE_DURATION,
    order_book::{Coin, MarketFilter, Px, Side, Snapshot, Sz, multi_book::OrderBooks, types::InnerOrder},
    prelude::*,
    types::{
        inner::InnerLevel,
        node_data::{Batch, NodeDataFill, NodeDataOrderDiff, NodeDataOrderStatus},
    },
};
use log::{error, info};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};
use tokio::process::Command;

use crate::SnapshotMode;

/// Configuration for snapshot fetching
#[derive(Debug, Clone)]
pub(super) struct SnapshotConfig {
    pub mode: SnapshotMode,
    pub docker_container: String,
    pub hlnode_binary: String,
    pub abci_state_path: Option<PathBuf>,
    pub snapshot_output_path: Option<PathBuf>,
    pub data_dir: PathBuf,
}

struct AbciSnapshotSource {
    host_path: PathBuf,
    container_path: String,
    height: u64,
}

/// Snapshot file plus the exact source height when it is known before parsing.
///
/// A raw CLI L4 snapshot has no height in the JSON body. It is only safe when
/// the source `abci_state` height is proven independently. For normal nodes we
/// use `data/periodic_abci_states/{date}/{height}.rmp`, whose file name is the
/// source height. Live visor height is intentionally not accepted: it can be
/// ahead of the frozen state file and would skip replay events.
pub(super) struct SnapshotArtifact {
    pub path: PathBuf,
    pub exact_source_height: u64,
}

pub(super) async fn fetch_l4_snapshot(config: &SnapshotConfig) -> Result<SnapshotArtifact> {
    info!("Triggering L4 snapshot (mode: {:?})...", config.mode);

    let dump_start = Instant::now();
    let source = resolve_abci_snapshot_source(config)?;
    let artifact = match config.mode {
        SnapshotMode::Docker => {
            // Docker mode: run command inside container
            // data_dir should be the path containing node_*_by_block directories
            // Snapshot goes to parent of data_dir (sibling to "data" folder)
            let parent_dir = config.data_dir.parent().unwrap_or(&config.data_dir);
            let output_path = config.snapshot_output_path.clone().unwrap_or_else(|| parent_dir.join("snapshot.json"));

            let output = Command::new("docker")
                .args(&[
                    "exec",
                    &config.docker_container,
                    "./hl-node",
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    "--include-trigger-orders",
                    &source.container_path,
                    "hl/snapshot.json",
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (docker mode)");
                }
                Err(e) => {
                    error!("Failed to execute docker command: {}", e);
                    return Err(e.into());
                }
            }

            SnapshotArtifact { path: output_path, exact_source_height: source.height }
        }
        SnapshotMode::Direct => {
            // Direct mode: run hl-node directly on host
            let output_path =
                config.snapshot_output_path.clone().unwrap_or_else(|| PathBuf::from("/tmp/hl_snapshot.json"));

            info!(
                "Running: {} --chain Mainnet compute-l4-snapshots --include-users --include-trigger-orders {} {}",
                &config.hlnode_binary,
                source.host_path.display(),
                output_path.display()
            );

            let output = Command::new(&config.hlnode_binary)
                .args(&[
                    "--chain",
                    "Mainnet",
                    "compute-l4-snapshots",
                    "--include-users",
                    "--include-trigger-orders",
                    source.host_path.to_str().unwrap_or(""),
                    output_path.to_str().unwrap_or(""),
                ])
                .output()
                .await;

            match output {
                Ok(out) => {
                    if !out.status.success() {
                        error!("hl-node compute-l4-snapshots failed: {}", String::from_utf8_lossy(&out.stderr));
                        error!("stdout: {}", String::from_utf8_lossy(&out.stdout));
                        return Err("hl-node compute-l4-snapshots failed".into());
                    }
                    info!("L4 snapshot computed successfully (direct mode)");
                }
                Err(e) => {
                    error!("Failed to execute hl-node command: {}", e);
                    return Err(e.into());
                }
            }

            SnapshotArtifact { path: output_path, exact_source_height: source.height }
        }
    };

    let dump_elapsed = dump_start.elapsed();
    RESYNC_PHASE_DURATION.with_label_values(&["fetch_dump"]).observe(dump_elapsed.as_secs_f64());
    info!("hl-node compute-l4-snapshots completed in {}ms (mode: {:?})", dump_elapsed.as_millis(), config.mode);

    // Verify file exists
    if artifact.path.exists() {
        info!("Snapshot file found at: {:?}", artifact.path);
        return Ok(artifact);
    }

    // Debug: List directory contents if file not found
    if let Some(parent) = artifact.path.parent() {
        error!("File not found. Listing directory {:?}:", parent);
        if let Ok(entries) = fs::read_dir(parent) {
            for entry in entries.flatten() {
                error!(" - {:?}", entry.path());
            }
        } else {
            error!("Failed to read directory {:?}", parent);
        }
    }

    Err("Snapshot file not created".into())
}

fn resolve_abci_snapshot_source(config: &SnapshotConfig) -> Result<AbciSnapshotSource> {
    if let Some(path) = &config.abci_state_path {
        let height = snapshot_height_from_path(path).ok_or_else(|| {
            format!(
                "--abci-state-path must point at a height-named state snapshot like \
                 periodic_abci_states/{{date}}/{{height}}.rmp; {} has no numeric height",
                path.display()
            )
        })?;
        return Ok(AbciSnapshotSource {
            host_path: path.clone(),
            container_path: path.to_string_lossy().into_owned(),
            height,
        });
    }

    latest_periodic_abci_state(&config.data_dir)
}

fn snapshot_height_from_path(path: &Path) -> Option<u64> {
    path.file_stem()?.to_str()?.parse().ok()
}

fn latest_periodic_abci_state(data_dir: &Path) -> Result<AbciSnapshotSource> {
    let root = data_dir.join("periodic_abci_states");
    let mut best: Option<(u64, PathBuf)> = None;

    for day in fs::read_dir(&root)
        .map_err(|err| format!("failed to read periodic abci state dir {}: {err}", root.display()))?
        .flatten()
    {
        let day_path = day.path();
        if !day_path.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&day_path).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("rmp") {
                continue;
            }
            let Some(height) = snapshot_height_from_path(&path) else {
                continue;
            };
            if best.as_ref().is_none_or(|(best_height, _)| height > *best_height) {
                best = Some((height, path));
            }
        }
    }

    let Some((height, host_path)) = best else {
        return Err(format!(
            "no height-named periodic abci snapshots found under {}; cannot safely label an L4 snapshot",
            root.display()
        )
        .into());
    };
    let rel = host_path.strip_prefix(data_dir).unwrap_or(&host_path);
    let container_path = Path::new("hl").join("data").join(rel).to_string_lossy().into_owned();
    Ok(AbciSnapshotSource { host_path, container_path, height })
}

/// Startup backfill floor that is coupled to the snapshot source.
///
/// We use the existing height-named periodic state snapshots emitted by the
/// node. This keeps the snapshot contents and replay floor coupled without
/// requiring any extra node endpoint or parsing the internal msgpack state.
pub(super) fn startup_backfill_min_height(config: &SnapshotConfig) -> Result<u64> {
    Ok(resolve_abci_snapshot_source(config)?.height)
}

/// Current node height from `visor_abci_state.json`, or None if unreadable.
/// Kept only for diagnostics/tests; do not use this to label an L4 snapshot.
#[cfg(test)]
pub(super) fn read_visor_height(visor_path: &Path) -> Option<u64> {
    let contents = fs::read_to_string(visor_path).ok()?;
    let visor: serde_json::Value = serde_json::from_str(&contents).ok()?;
    visor["height"].as_u64()
}

impl L2SnapshotParams {
    pub(crate) const fn new(n_sig_figs: Option<u32>, mantissa: Option<u64>) -> Self {
        Self { n_sig_figs, mantissa }
    }
}

/// Build the requested L2 aggregation variants for a single coin's order book.
/// Only the shapes in `active` are produced (instead of all seven), so a server
/// whose clients use few variants does proportionally less work.
///
/// Every variant is capped at `MAX_LEVELS` per side. Subscription validation
/// rejects `n_levels > MAX_LEVELS`, so deeper levels are pure waste in CPU,
/// memory, and broadcast Arc size (BTC: ~500 -> 100 levels/side).
///
fn merge_level(map: &mut BTreeMap<Px, (Sz, usize)>, px: Px, sz: Sz, n: usize) {
    map.entry(px)
        .and_modify(|(total_sz, total_n)| {
            *total_sz = *total_sz + sz;
            *total_n += n;
        })
        .or_insert((sz, n));
}

fn levels_from_map(map: BTreeMap<Px, (Sz, usize)>, side: Side, cap: Option<usize>) -> Vec<InnerLevel> {
    let mut levels = Vec::new();
    if cap == Some(0) {
        return levels;
    }
    match side {
        Side::Ask => {
            for (px, (sz, n)) in map {
                if cap == Some(levels.len()) {
                    break;
                }
                levels.push(InnerLevel { px, sz, n });
            }
        }
        Side::Bid => {
            for (px, (sz, n)) in map.into_iter().rev() {
                if cap == Some(levels.len()) {
                    break;
                }
                levels.push(InnerLevel { px, sz, n });
            }
        }
    }
    levels
}

fn synthetic_l2_base_for_coin<O: InnerOrder>(
    order_books: &OrderBooks<O>,
    coin: &Coin,
    cap: Option<usize>,
) -> Snapshot<InnerLevel> {
    let mut bids = BTreeMap::new();
    let mut asks = BTreeMap::new();

    if let Some(book) = order_books.as_ref().get(coin) {
        let raw = book.to_l2_snapshot(cap, None, None);
        let [raw_bids, raw_asks] = raw.as_ref();
        for level in raw_bids {
            merge_level(&mut bids, level.px, level.sz, level.n);
        }
        for level in raw_asks {
            merge_level(&mut asks, level.px, level.sz, level.n);
        }
    }

    // HIP-4 public l2Book exposes complete-set liquidity: an order on one side
    // coin is also executable against the sibling at `1 - px` with the side
    // flipped. Keep L4/raw streams untouched; synthesize only the L2/BBO view.
    if let Some(sibling) = coin.hip4_sibling() {
        if let Some(book) = order_books.as_ref().get(&sibling) {
            let raw = book.to_l2_snapshot(cap, None, None);
            let [sibling_bids, sibling_asks] = raw.as_ref();
            for level in sibling_asks {
                if let Some(px) = level.px.one_minus() {
                    merge_level(&mut bids, px, level.sz, level.n);
                }
            }
            for level in sibling_bids {
                if let Some(px) = level.px.one_minus() {
                    merge_level(&mut asks, px, level.sz, level.n);
                }
            }
        }
    }

    Snapshot::new([levels_from_map(bids, Side::Bid, cap), levels_from_map(asks, Side::Ask, cap)])
}

fn has_synthetic_l2_book<O: InnerOrder>(order_books: &OrderBooks<O>, coin: &Coin) -> bool {
    order_books.as_ref().contains_key(coin)
        || coin.hip4_sibling().is_some_and(|sibling| order_books.as_ref().contains_key(&sibling))
}

fn synthetic_l2_universe<O: InnerOrder>(order_books: &OrderBooks<O>) -> HashSet<Coin> {
    let mut coins: HashSet<Coin> = order_books.as_ref().keys().cloned().collect();
    for coin in order_books.as_ref().keys() {
        if let Some(sibling) = coin.hip4_sibling() {
            coins.insert(sibling);
        }
    }
    coins
}

fn expand_with_hip4_siblings(coins: &HashSet<Coin>) -> HashSet<Coin> {
    let mut expanded = coins.clone();
    for coin in coins {
        if let Some(sibling) = coin.hip4_sibling() {
            expanded.insert(sibling);
        }
    }
    expanded
}

fn compute_l2_variants_for_coin<O: InnerOrder>(
    order_books: &OrderBooks<O>,
    coin: &Coin,
    active: &HashSet<L2SnapshotParams>,
) -> HashMap<L2SnapshotParams, Snapshot<InnerLevel>> {
    use crate::types::subscription::MAX_LEVELS;
    let mut out = HashMap::new();
    if active.is_empty() {
        return out;
    }
    let cap = Some(MAX_LEVELS);

    let base_params = L2SnapshotParams { n_sig_figs: None, mantissa: None };
    // Mirror the full HIP-4 book before grouping. Truncating raw levels first
    // loses depth when many price levels collapse into one coarse bucket.
    if coin.is_hip4() {
        let base = synthetic_l2_base_for_coin(order_books, coin, None);
        for params in active.iter().chain(std::iter::once(&base_params)) {
            out.insert(*params, base.to_l2_snapshot(cap, params.n_sig_figs, params.mantissa));
        }
    } else if let Some(book) = order_books.as_ref().get(coin) {
        for params in active.iter().chain(std::iter::once(&base_params)) {
            out.insert(*params, book.to_l2_snapshot(cap, params.n_sig_figs, params.mantissa));
        }
    }
    out
}

/// Incremental rebuild: recomputes variants only for `changed_coins`, reuses
/// the cached `Arc<HashMap>` for every other coin. Returns a fresh `L2Snapshots`
/// holding `Arc::clone`d entries — the outgoing broadcast message and the
/// listener-side cache share the underlying inner maps, so unchanged coins
/// cost a single Arc bump per broadcast instead of a full level-vector clone.
/// Also returned: the set of coins actually recomputed (connections use it to
/// skip subscriptions whose cached payload is still current) and whether the
/// cached coin set changed (a coin appeared or was evicted), which tells the
/// caller to rebuild the shared universe.
///
/// Also evicts cache entries for coins no longer present in `order_books`
/// (e.g. when a coin is delisted and the multi-book removes it). Without
/// this the cache would grow monotonically with the universe size.
/// Cap on present-but-uncached coins backfilled per flush. After a snapshot
/// install (or an active-shape change) clears the cache, the full universe
/// would otherwise be rebuilt in one rayon burst while the listener lock is
/// held; the cap spreads that backfill across a few throttle windows.
/// Uncapped coins are re-detected as uncached and picked up by subsequent
/// flushes, so convergence is automatic. Dirty coins are never capped: a
/// coin that actually changed must not be served stale.
const L2_BACKFILL_COINS_PER_FLUSH: usize = 32;

pub(super) fn compute_l2_snapshots_incremental<O: InnerOrder + Send + Sync>(
    order_books: &OrderBooks<O>,
    changed_coins: &HashSet<Coin>,
    active: &HashSet<L2SnapshotParams>,
    cache: &mut HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>>,
) -> (L2Snapshots, HashSet<Coin>, bool) {
    /// Below this many dirty coins (the common case is 1-3 per 50ms flush),
    /// rayon's task-dispatch overhead exceeds the rebuild work itself.
    const PAR_COMPUTE_THRESHOLD: usize = 8;

    let synthetic_universe = synthetic_l2_universe(order_books);
    let expanded_changed_coins = expand_with_hip4_siblings(changed_coins);

    // Evict stale entries.
    let len_before_evict = cache.len();
    cache.retain(|coin, _| synthetic_universe.contains(coin));
    let mut coin_set_changed = cache.len() != len_before_evict;

    // Determine which coins we actually need to (re)compute: anything in
    // `changed_coins` that the book still contains, plus any present-but-uncached
    // coins (first-time broadcast after a snapshot reset).
    let mut to_compute: Vec<Coin> =
        expanded_changed_coins.iter().filter(|c| has_synthetic_l2_book(order_books, c)).cloned().collect();
    let mut backfilled = 0usize;
    for coin in &synthetic_universe {
        if !cache.contains_key(coin) && !expanded_changed_coins.contains(coin) {
            if backfilled >= L2_BACKFILL_COINS_PER_FLUSH {
                break;
            }
            backfilled += 1;
            to_compute.push(coin.clone());
        }
    }
    coin_set_changed |= to_compute.iter().any(|coin| !cache.contains_key(coin));

    // Recompute the coins we need, building only the subscribed shapes; fan
    // out to rayon only for genuinely large rebuilds (post-snapshot recompute).
    let build = |coin: Coin| {
        has_synthetic_l2_book(order_books, &coin).then(|| {
            let variants = compute_l2_variants_for_coin(order_books, &coin, active);
            (coin, Arc::new(variants))
        })
    };
    let updates: Vec<(Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>)> =
        if to_compute.len() < PAR_COMPUTE_THRESHOLD {
            to_compute.into_iter().filter_map(build).collect()
        } else {
            to_compute.into_par_iter().filter_map(build).collect()
        };
    let mut recomputed = HashSet::with_capacity(updates.len());
    for (coin, arc) in updates {
        recomputed.insert(coin.clone());
        cache.insert(coin, arc);
    }
    // A dirty coin whose book is GONE (last order cancelled -> multi-book
    // evicted it) still counts as recomputed: connections must be told the
    // book is now empty, or they keep the last snapshot forever.
    for coin in &expanded_changed_coins {
        if !synthetic_universe.contains(coin) {
            recomputed.insert(coin.clone());
        }
    }

    // Build the outgoing L2Snapshots from the cache. Each entry is an Arc::clone -
    // O(coins) cheap atomic bumps, no level data is copied.
    let snapshot: HashMap<Coin, Arc<HashMap<L2SnapshotParams, Snapshot<InnerLevel>>>> =
        cache.iter().map(|(c, arc)| (c.clone(), Arc::clone(arc))).collect();
    (L2Snapshots(snapshot), recomputed, coin_set_changed)
}

#[derive(Clone)]
pub(super) enum EventBatch {
    Orders(Batch<NodeDataOrderStatus>),
    BookDiffs(Batch<NodeDataOrderDiff>),
    Fills(Batch<NodeDataFill>),
}

impl EventBatch {
    pub(super) fn filter_market(self, market_filter: MarketFilter) -> Option<Self> {
        let batch = match self {
            Self::Orders(batch) => {
                Self::Orders(batch.filter_events(|status| market_filter.matches(&Coin::new(&status.order.coin))))
            }
            Self::BookDiffs(batch) => Self::BookDiffs(batch.filter_events(|diff| market_filter.matches(&diff.coin()))),
            Self::Fills(batch) => {
                Self::Fills(batch.filter_events(|fill| market_filter.matches(&Coin::new(&fill.1.coin))))
            }
        };
        let events_len = match &batch {
            Self::Orders(batch) => batch.events_len(),
            Self::BookDiffs(batch) => batch.events_len(),
            Self::Fills(batch) => batch.events_len(),
        };
        (events_len > 0).then_some(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        order_book::{Px, Side, Sz, multi_book::Snapshots},
        types::inner::InnerL4Order,
    };
    use alloy::primitives::Address;
    use std::{collections::HashSet, path::PathBuf};

    fn order(oid: u64, coin: &str, side: Side, sz: &str, px: &str) -> InnerL4Order {
        InnerL4Order {
            user: Address::new([0; 20]),
            coin: Coin::new(coin),
            side,
            limit_px: Px::parse_from_str(px).unwrap(),
            sz: Sz::parse_from_str(sz).unwrap(),
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
        }
    }

    /// The full set of supported L2 variant shapes (what the listener built before
    /// subscription-aware computation). Used by tests to exercise all variants.
    fn all_params() -> HashSet<L2SnapshotParams> {
        [
            L2SnapshotParams::new(None, None),
            L2SnapshotParams::new(Some(5), None),
            L2SnapshotParams::new(Some(5), Some(2)),
            L2SnapshotParams::new(Some(5), Some(5)),
            L2SnapshotParams::new(Some(4), None),
            L2SnapshotParams::new(Some(3), None),
            L2SnapshotParams::new(Some(2), None),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn test_read_visor_height() {
        let dir = std::env::temp_dir().join(format!("obs_visor_test_{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("visor_abci_state.json");

        fs::write(&path, r#"{"height": 12345, "other": "x"}"#).unwrap();
        assert_eq!(read_visor_height(&path), Some(12345));

        fs::write(&path, "not json").unwrap();
        assert_eq!(read_visor_height(&path), None);

        assert_eq!(read_visor_height(&dir.join("missing.json")), None);
        fs::remove_dir_all(&dir).ok();
    }

    fn snapshot_config(mode: SnapshotMode, abci_state_path: Option<PathBuf>) -> SnapshotConfig {
        SnapshotConfig {
            mode,
            docker_container: "hyperliquid_hlnode".to_string(),
            hlnode_binary: "hl-node".to_string(),
            abci_state_path,
            snapshot_output_path: None,
            data_dir: PathBuf::from("/var/lib/hl/data"),
        }
    }

    #[test]
    fn test_startup_backfill_floor_uses_periodic_abci_height() {
        let config = snapshot_config(
            SnapshotMode::Direct,
            Some(PathBuf::from("/var/lib/hl/data/periodic_abci_states/20260707/123450000.rmp")),
        );
        assert_eq!(startup_backfill_min_height(&config).unwrap(), 123_450_000);
    }

    #[test]
    fn test_startup_backfill_rejects_explicit_live_abci_without_height() {
        let config =
            snapshot_config(SnapshotMode::Direct, Some(PathBuf::from("/var/lib/hl/hyperliquid_data/abci_state.rmp")));
        let err = startup_backfill_min_height(&config).unwrap_err().to_string();
        assert!(err.contains("must point at a height-named state snapshot"), "{err}");
    }

    #[test]
    fn test_startup_backfill_floor_auto_discovers_latest_periodic_snapshot() {
        let dir = std::env::temp_dir().join(format!("obs_periodic_abci_test_{}", std::process::id()));
        let day1 = dir.join("periodic_abci_states/20260706");
        let day2 = dir.join("periodic_abci_states/20260707");
        fs::create_dir_all(&day1).unwrap();
        fs::create_dir_all(&day2).unwrap();
        fs::write(day1.join("123.rmp"), b"").unwrap();
        fs::write(day2.join("122.rmp"), b"").unwrap();
        fs::write(day2.join("456.rmp"), b"").unwrap();
        fs::write(day2.join("not-a-height.rmp"), b"").unwrap();

        let config = SnapshotConfig {
            mode: SnapshotMode::Direct,
            docker_container: "hyperliquid_hlnode".to_string(),
            hlnode_binary: "hl-node".to_string(),
            abci_state_path: None,
            snapshot_output_path: None,
            data_dir: dir.clone(),
        };
        assert_eq!(startup_backfill_min_height(&config).unwrap(), 456);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_l2_variants_are_capped_to_max_levels() {
        use crate::types::subscription::MAX_LEVELS;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        // Add more than MAX_LEVELS distinct price levels on each side.
        for i in 0..(MAX_LEVELS + 50) {
            let bid_px = format!("{}", 1000 + i);
            let ask_px = format!("{}", 100_000 + i);
            books.add_order(order(i as u64, "BTC", Side::Bid, "1", &bid_px));
            books.add_order(order((1_000_000 + i) as u64, "BTC", Side::Ask, "1", &ask_px));
        }

        let coin = Coin::new("BTC");
        let variants = compute_l2_variants_for_coin(&books, &coin, &all_params());
        let base = variants.get(&L2SnapshotParams::new(None, None)).unwrap();
        let [bids, asks] = base.as_ref();
        assert!(bids.len() <= MAX_LEVELS, "base bids capped: {} <= {}", bids.len(), MAX_LEVELS);
        assert!(asks.len() <= MAX_LEVELS, "base asks capped: {} <= {}", asks.len(), MAX_LEVELS);
        // Every aggregated variant is also bounded by the cap.
        for snap in variants.values() {
            let [b, a] = snap.as_ref();
            assert!(b.len() <= MAX_LEVELS && a.len() <= MAX_LEVELS, "an aggregated variant exceeds the cap");
        }
    }

    #[test]
    fn test_incremental_reuses_arc_for_unchanged_coins() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        // First call seeds the cache for both coins.
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert_eq!(cache.len(), 2);
        let btc_first = Arc::clone(cache.get(&Coin::new("BTC")).unwrap());
        let eth_first = Arc::clone(cache.get(&Coin::new("ETH")).unwrap());

        // Mark BTC changed; ETH unchanged. ETH's Arc must be the same object.
        let changed: HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        books.add_order(order(3, "BTC", Side::Bid, "2", "50001"));
        let _ = compute_l2_snapshots_incremental(&books, &changed, &all_params(), &mut cache);

        let btc_after = cache.get(&Coin::new("BTC")).unwrap();
        let eth_after = cache.get(&Coin::new("ETH")).unwrap();
        assert!(!Arc::ptr_eq(&btc_first, btc_after), "BTC should have been recomputed");
        assert!(Arc::ptr_eq(&eth_first, eth_after), "ETH must be Arc-shared (not recomputed)");
    }

    #[test]
    fn test_incremental_rebuilds_full_accumulated_set_not_just_triggering_coin() {
        // Regression for the L2 conflation bug: when a coin changes during a
        // throttle-suppressed window, the broadcast must rebuild the FULL accumulated
        // set of dirty coins, not just the coin in the triggering event. Passing the
        // accumulated set {A, B} must recompute both - A must NOT be served stale.
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "A", Side::Bid, "1", "100"));
        books.add_order(order(2, "B", Side::Bid, "1", "200"));

        let mut cache = HashMap::new();
        // Seed both coins.
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        let a_seed = Arc::clone(cache.get(&Coin::new("A")).unwrap());
        let b_seed = Arc::clone(cache.get(&Coin::new("B")).unwrap());

        // A changes during a suppressed window; B changes in the triggering event.
        // The conflation buffer accumulates both.
        books.add_order(order(3, "A", Side::Bid, "5", "101"));
        books.add_order(order(4, "B", Side::Bid, "5", "201"));

        let dirty: HashSet<Coin> = ["A", "B"].iter().map(|c| Coin::new(c)).collect();
        let _ = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);

        assert!(
            !Arc::ptr_eq(&a_seed, cache.get(&Coin::new("A")).unwrap()),
            "A changed during the suppressed window and must be rebuilt, not served stale"
        );
        assert!(
            !Arc::ptr_eq(&b_seed, cache.get(&Coin::new("B")).unwrap()),
            "B changed in the triggering event and must be rebuilt"
        );
    }

    #[test]
    fn test_hip4_l2_merges_complementary_sibling_liquidity() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "#7510", Side::Bid, "10", "0.8"));
        books.add_order(order(2, "#7510", Side::Ask, "2", "0.9"));
        books.add_order(order(3, "#7511", Side::Bid, "3", "0.1")); // mirrored as #7510 ask 0.9
        books.add_order(order(4, "#7511", Side::Ask, "4", "0.3")); // mirrored as #7510 bid 0.7

        let variants = compute_l2_variants_for_coin(&books, &Coin::new("#7510"), &all_params());
        let base = variants.get(&L2SnapshotParams::new(None, None)).unwrap();
        let [bids, asks] = base.as_ref();
        let bid_levels: Vec<_> = bids.iter().map(|l| (l.px.to_str(), l.sz.to_str(), l.n)).collect();
        let ask_levels: Vec<_> = asks.iter().map(|l| (l.px.to_str(), l.sz.to_str(), l.n)).collect();

        assert_eq!(bid_levels, vec![("0.8".to_string(), "10".to_string(), 1), ("0.7".to_string(), "4".to_string(), 1)]);
        assert_eq!(ask_levels, vec![("0.9".to_string(), "5".to_string(), 2)]);
    }

    #[test]
    fn test_incremental_serves_stale_when_changed_coin_omitted() {
        // Documents the pre-fix behavior the conflation buffer eliminates: if a
        // changed coin (A) is omitted from the passed set (as happened when A's change
        // landed in a throttle-suppressed event and was discarded), A is served from
        // its stale cached Arc even though the book changed.
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "A", Side::Bid, "1", "100"));
        books.add_order(order(2, "B", Side::Bid, "1", "200"));

        let mut cache = HashMap::new();
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        let a_seed = Arc::clone(cache.get(&Coin::new("A")).unwrap());

        // A's book changes, but only B is passed as changed (A's change was dropped).
        books.add_order(order(3, "A", Side::Bid, "5", "101"));
        let only_b: HashSet<Coin> = std::iter::once(Coin::new("B")).collect();
        let _ = compute_l2_snapshots_incremental(&books, &only_b, &all_params(), &mut cache);

        assert!(
            Arc::ptr_eq(&a_seed, cache.get(&Coin::new("A")).unwrap()),
            "demonstrates the stale-serve bug: A's change is invisible when omitted from the changed set"
        );
    }

    #[test]
    fn test_incremental_reports_recomputed_and_coin_set_changes() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        let (_, recomputed, changed) =
            compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(changed, "first build introduces coins to the cache");
        assert!(recomputed.contains("BTC") && recomputed.contains("ETH"));

        // Only BTC dirty: recomputed is exactly {BTC}, coin set unchanged.
        let dirty: HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        let (_, recomputed, changed) = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);
        assert!(!changed, "no coin appeared or disappeared");
        assert_eq!(recomputed.len(), 1);
        assert!(recomputed.contains("BTC"));

        // Evicting a coin flags a coin-set change (universe must be rebuilt).
        books.cancel_order(crate::order_book::Oid::new(1), Coin::new("BTC"));
        let (_, recomputed, changed) =
            compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(changed, "eviction must flag a universe change");
        assert!(recomputed.is_empty());
    }

    #[test]
    fn test_backfill_is_capped_per_flush_and_converges() {
        // Post-install: empty cache, no dirty coins. Each flush must backfill
        // at most L2_BACKFILL_COINS_PER_FLUSH coins (bounding the under-lock
        // rayon burst) and repeated flushes must converge to the full universe.
        let n_coins = 3 * L2_BACKFILL_COINS_PER_FLUSH;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        for i in 0..n_coins {
            books.add_order(order(i as u64, &format!("C{i}"), Side::Bid, "1", "100"));
        }

        let mut cache = HashMap::new();
        let (_, recomputed, changed) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(changed, "backfill introduces coins to the cache");
        assert_eq!(recomputed.len(), L2_BACKFILL_COINS_PER_FLUSH, "backfill must be capped per flush");
        assert_eq!(cache.len(), L2_BACKFILL_COINS_PER_FLUSH);

        let mut flushes = 1;
        while cache.len() < n_coins {
            let (_, recomputed, _) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
            assert!(recomputed.len() <= L2_BACKFILL_COINS_PER_FLUSH);
            assert!(!recomputed.is_empty(), "the ramp must make progress every flush");
            flushes += 1;
        }
        assert_eq!(flushes, 3, "the ramp must converge in universe/cap flushes");
        // Converged: nothing left to backfill.
        let (_, recomputed, _) = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(recomputed.is_empty());
    }

    #[test]
    fn test_dirty_coins_are_never_capped() {
        // Every dirty coin must be rebuilt in the flush that drains it, even if
        // there are more dirty coins than the backfill cap - the cap only
        // applies to present-but-uncached (backfill) coins.
        let n_coins = 2 * L2_BACKFILL_COINS_PER_FLUSH;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        let mut dirty = HashSet::new();
        for i in 0..n_coins {
            books.add_order(order(i as u64, &format!("C{i}"), Side::Bid, "1", "100"));
            dirty.insert(Coin::new(&format!("C{i}")));
        }

        let mut cache = HashMap::new();
        let (_, recomputed, _) = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);
        assert_eq!(recomputed.len(), n_coins, "dirty coins must all be rebuilt in one flush");
    }

    #[test]
    fn test_dirty_evicted_coin_is_reported_recomputed() {
        // A coin whose last order was cancelled is dirty AND gone from the
        // book. It must still appear in the recomputed set so connections are
        // told the book is now empty instead of serving the stale snapshot.
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        let mut cache = HashMap::new();
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);

        books.cancel_order(crate::order_book::Oid::new(1), Coin::new("BTC")); // book evicted
        let dirty: HashSet<Coin> = std::iter::once(Coin::new("BTC")).collect();
        let (snapshots, recomputed, _) = compute_l2_snapshots_incremental(&books, &dirty, &all_params(), &mut cache);
        assert!(recomputed.contains("BTC"), "evicted dirty coin must be reported so subscribers get an empty book");
        assert!(!snapshots.as_ref().contains_key(&Coin::new("BTC")), "the snapshot map no longer carries the coin");
    }

    #[test]
    fn test_incremental_evicts_removed_coins() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        books.add_order(order(2, "ETH", Side::Bid, "1", "3000"));

        let mut cache = HashMap::new();
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(cache.contains_key(&Coin::new("BTC")));

        // Cancel BTC's only order — the multi-book evicts the empty book, which
        // means our cache must also drop the entry on the next incremental call.
        books.cancel_order(crate::order_book::Oid::new(1), Coin::new("BTC"));
        let _ = compute_l2_snapshots_incremental(&books, &HashSet::new(), &all_params(), &mut cache);
        assert!(!cache.contains_key(&Coin::new("BTC")), "BTC entry should have been evicted from the cache");
        assert!(cache.contains_key(&Coin::new("ETH")));
    }

    #[test]
    fn test_compute_only_builds_requested_variants() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));

        let mut active = HashSet::new();
        active.insert(L2SnapshotParams::new(Some(5), None));
        let variants = compute_l2_variants_for_coin(&books, &Coin::new("BTC"), &active);

        // The requested shape plus the always-present raw base; nothing else.
        assert!(variants.contains_key(&L2SnapshotParams::new(Some(5), None)), "requested variant built");
        assert!(variants.contains_key(&L2SnapshotParams::new(None, None)), "raw base always present");
        assert!(!variants.contains_key(&L2SnapshotParams::new(Some(2), None)), "unrequested variant not built");
        assert!(!variants.contains_key(&L2SnapshotParams::new(Some(5), Some(5))), "unrequested variant not built");
        assert_eq!(variants.len(), 2);
    }

    #[test]
    fn test_compute_empty_active_builds_nothing() {
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        books.add_order(order(1, "BTC", Side::Bid, "1", "50000"));
        let variants = compute_l2_variants_for_coin(&books, &Coin::new("BTC"), &HashSet::new());
        assert!(variants.is_empty(), "empty active set computes no variants");
    }

    #[test]
    fn test_coarse_variant_aggregates_full_depth_not_truncated_base() {
        // Regression: aggregated variants used to be derived from the raw base
        // AFTER it was truncated to MAX_LEVELS raw levels. The top raw levels
        // cluster near the mid, so coarse groupings (nSigFigs=2 -> $1000-wide
        // buckets here) collapsed into 1-2 buckets and all deep far-from-mid
        // liquidity vanished. Aggregation must run over the FULL book and
        // truncate by aggregated buckets, like HL's public API.
        use crate::types::subscription::MAX_LEVELS;
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        let mut oid = 0u64;
        // More than MAX_LEVELS raw bid levels packed within ~$120 of the mid...
        for i in 0..(MAX_LEVELS + 20) {
            books.add_order(order(oid, "BTC", Side::Bid, "1", &format!("{}", 64_931 + i)));
            oid += 1;
        }
        // ...plus deep liquidity far below the mid that the truncated base never saw.
        for deep_px in [50_000, 40_000, 30_000, 20_000] {
            books.add_order(order(oid, "BTC", Side::Bid, "1", &format!("{deep_px}")));
            oid += 1;
        }
        books.add_order(order(oid, "BTC", Side::Ask, "1", "65100"));

        let mut active = HashSet::new();
        active.insert(L2SnapshotParams::new(Some(2), None));
        let variants = compute_l2_variants_for_coin(&books, &Coin::new("BTC"), &active);
        let [bids, _] = variants.get(&L2SnapshotParams::new(Some(2), None)).unwrap().as_ref();

        // 64931..=65050 buckets to {65000, 64000}; the deep levels add 4 more.
        assert_eq!(bids.len(), 6, "coarse buckets must cover the full book depth, got {bids:?}");
        let total_sz: u64 = bids.iter().map(|l| l.sz.value()).sum();
        let expected_sz = Sz::parse_from_str(&format!("{}", MAX_LEVELS + 24)).unwrap().value();
        assert_eq!(total_sz, expected_sz, "no liquidity may be dropped by aggregation");
    }

    #[test]
    fn test_requested_variant_matches_full_compute() {
        // A single-shape build must equal what the all-variants build produces
        // for the same shape (subscription-aware computation is value-correct).
        let mut books: OrderBooks<InnerL4Order> = OrderBooks::from_snapshots(Snapshots::new(HashMap::new()), true);
        for i in 0..20 {
            books.add_order(order(i, "BTC", Side::Bid, "1", &format!("{}", 50000 - i)));
            books.add_order(order(1000 + i, "BTC", Side::Ask, "1", &format!("{}", 50100 + i)));
        }
        let full = compute_l2_variants_for_coin(&books, &Coin::new("BTC"), &all_params());
        for shape in all_params() {
            let mut one = HashSet::new();
            one.insert(shape);
            let single = compute_l2_variants_for_coin(&books, &Coin::new("BTC"), &one);
            // InnerLevel has no PartialEq; compare via Debug rendering of the levels.
            assert_eq!(
                format!("{:?}", single.get(&shape).map(Snapshot::as_ref)),
                format!("{:?}", full.get(&shape).map(Snapshot::as_ref)),
                "variant must match the all-variants build"
            );
        }
    }
}
