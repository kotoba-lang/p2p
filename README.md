# p2p

[![CI](https://github.com/kotoba-lang/p2p/actions/workflows/ci.yml/badge.svg)](https://github.com/kotoba-lang/p2p/actions/workflows/ci.yml)

**P2P graph-sync protocol for kotoba's content-addressed commit chains —
pure state machine, transport injected, verified on JVM and real
ClojureScript (shadow-cljs).**

Composition, not reimplementation:

| layer | supplies |
|---|---|
| [`kotoba-lang/net`](https://github.com/kotoba-lang/net) | gossip routing semantics (content-hash dedup, deterministic fanout) + bitswap `commits-since` delta |
| [`kotoba-lang/chain`](https://github.com/kotoba-lang/chain) (renamed from `commit-dag`, ADR-2607050800) | chain walk + tamper/seq verification |
| [`kotoba-lang/kotoba-client`](https://github.com/kotoba-lang/kotoba-client) | CID-verified block ingest + the generic tag-42 missing-blocks walk |

The tag-42 migration ([`kotoba-lang/ipld`](https://github.com/kotoba-lang/ipld))
is what makes this protocol small: chain `prev`, snapshot `state`, index
roots and tree children are all real IPLD links, so **"sync a graph"
collapses to "hydrate the announced head CID to convergence, then
`verify-chain`"** — there is no per-schema transfer logic anywhere.

## Protocol

Five message types, all handled by the pure step
`(sync/handle node msg) → {:node node' :effects [{:to peer :msg m} …]}`:

```
:head-announce ──gossip fanout + dedup──▶ every mesh peer
      │ (announced seq > local seq)
      ▼
:want-since ──▶ origin           bitswap WantSince over the chain log
      ▼
:commits ◀── delta entries + head
      ▼
:want-blocks ──▶ origin          missing CIDs from the generic tag-42 walk
      ▼
:blocks ◀── {cid bytes}          CID-verified ingest; loop until no missing,
      ▼                          then verify-chain → adopt → re-announce
adopt + re-announce (hop-by-hop propagation)
```

Trust model: blocks are CID-verified on ingest (a lying peer's bytes are
skipped, never stored) and a head is adopted only after `verify-chain`
passes over fully-local blocks. A malicious peer can stall sync
(liveness) — including the repeated-want stall guard dropping the pending
head — but never corrupt state (safety). Trust lives in the CID, not the
transport, exactly as the browser read-plane ADR put it.

## Use

```clojure
(require '[kotoba.p2p.sync :as sync]
         '[kotoba.p2p.loopback :as lb])

(def a (-> (sync/new-node "a" store-a)          ; {:put! :get-fn} ports
           (sync/add-peer "b" #{"my-graph"})
           (sync/set-head "my-graph" chain-cid 1)))
(def b (-> (sync/new-node "b" store-b)
           (sync/add-peer "a" #{"my-graph"})))

(def net (lb/announce-and-run (lb/network {"a" a "b" b}) "a" "my-graph"))
(sync/head (get-in net [:nodes "b"]) "my-graph") ;=> {:head-cid … :seq 1}
```

`kotoba.p2p.loopback` is the in-memory reference transport (deterministic,
breadth-first, quiescence-bounded). A real transport (QUIC / WebRTC /
WebTransport / HTTP long-poll) implements the same two duties — deliver
`:effects`, feed incoming messages to `sync/handle` — as host glue outside
this repo.

## Signed head announces

`new-node` optionally takes `:sign-announce` (fn [msg] -> msg', applied to
every self-originated announce before it's gossiped) and
`:verify-announce?` (fn [msg] -> boolean, checked before a peer's announce
is trusted enough to chase with `want-since` — an unverified announce is
still relayed, gossip-style, just not acted on locally). Both default to
a no-op (`identity` / `(constantly true)`), so 2-arity `new-node` callers
are unaffected. This namespace carries no crypto/identity scheme of its
own — `kotoba-lang/kotoba-rad`'s `kotoba-rad.announce` provides a
did:key/sigref-based pair of hooks that plug in here directly.

## What is NOT in this landing

Tracked follow-ups, not silently omitted:

- **Real wire transports** and peer discovery (Kademlia-equivalent) —
  host adapters over the effect/message seam.
- **Prolly-tree range diff** — `:want-blocks` ships whole missing subtrees;
  structural diff between two roots would cut transfer for
  mostly-shared trees (the Dolt/Noms trick the superproject ADR records).

## Test

The e2e suite syncs a **real kotobase-peer commit chain** (`kotobase-engine`
until ADR-2607050700 renamed it; transact → commit! ×2) between in-memory
nodes: two-node convergence + local
re-read via prolly-tree on the receiver's store, three-node line topology
(hop-by-hop propagation through re-announce), stale/duplicate announce
inertness, and a lying-peer safety/termination case.

```bash
clojure -M:test                     # JVM
npm install && npm run test:cljs    # real ClojureScript (shadow-cljs node-test)
```

## License

MIT
