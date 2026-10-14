# HFT Architecture Lab

A React + TypeScript simulator for evaluating four software architectures for a high-frequency trading brokerage pipeline:

- Pipe-and-Filter
- Microkernel
- Layered
- Shared Memory / Blackboard

The app lets users tune workload, network, architecture, and fault-injection parameters. **Run simulation** processes a reproducible, bounded sample of market events and order requests through per-architecture stage queues, service workers, network delays/retries, and fault injection. It reports p50/p95/p99 latency, completed/rejected order estimates, throughput, capacity, reliability, bottlenecks, and a ranked architecture recommendation.

The sample is capped so large configured workloads do not freeze the browser; projected request counts are scaled to the selected simulation duration. The displayed seed makes a run reproducible for the same inputs. The composite score weights p99 latency at 35%, throughput at 25%, completion reliability at 20%, fault tolerance at 10%, and modeled capacity at 10%. Order latency percentiles describe successful orders; rejected orders and failure counts are reported separately. An SLO pass requires p99 below 1 ms, throughput at least 95% of requested order rate, and at least 95% successful completion.

## Enable live stock trades

The live tape streams trade prints from Alpaca's IEX market-data feed. It requires Alpaca market-data credentials; it does not place orders. IEX is a single-exchange feed and is not a consolidated SIP feed. Availability and entitlement depend on the Alpaca account and subscription.

1. Create or use an Alpaca account with access to the IEX real-time stock feed.
2. Copy `.env.example` to `.env` and set `ALPACA_API_KEY` and `ALPACA_API_SECRET`.
3. Start the app with the command below, then choose ticker symbols and select **Start stream**.

The API credentials are read by the local Node relay and are never sent to the browser. Do not expose the local relay to untrusted networks. `.env` is ignored by Git.

When a simulation is run while the stream is active, the captured observed trade rate, recent trade sizes, and within-symbol price changes contribute to that run's workload sample. The displayed architecture latencies, capacity, failures, and scores are modeled simulation results; they are not measured end-to-end trading performance. A provider trade timestamp is shown in the tape. Real order submission and fills are not simulated by this feed.

## Run locally

```bash
npm install
npm run dev
```

This starts Vite and the local WebSocket relay. Open `http://localhost:5173/`.

## Build

```bash
npm run build
```

## Notes

All output is labeled as simulated results and is intended for architectural comparison rather than real-world HFT benchmarking.
