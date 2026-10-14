# HFT Architecture Lab

A React + TypeScript simulator for evaluating four software architectures for a high-frequency trading brokerage pipeline:

- Pipe-and-Filter
- Microkernel
- Layered
- Shared Memory / Blackboard

The app lets users tune workload, network, and fault-injection parameters, then compares each architecture on end-to-end latency, throughput, reliability, and suitability for a sub-millisecond trading target.

## Enable live stock trades

The live tape streams trade prints from Alpaca's IEX market-data feed. It requires Alpaca market-data credentials; it does not place orders. IEX is a single-exchange feed and is not a consolidated SIP feed. Availability and entitlement depend on the Alpaca account and subscription.

1. Create or use an Alpaca account with access to the IEX real-time stock feed.
2. Copy `.env.example` to `.env` and set `ALPACA_API_KEY` and `ALPACA_API_SECRET`.
3. Start the app with the command below, then choose ticker symbols and select **Start stream**.

The API credentials are read by the local Node relay and are never sent to the browser. Do not expose the local relay to untrusted networks. `.env` is ignored by Git.

While the stream is connected and trades arrive, observed incoming trades per second calibrate the simulator's market-event-rate input. The displayed architecture latencies, capacity, failures, and scores remain modeled estimates; they are not measured end-to-end trading performance. A provider trade timestamp is shown in the tape. Real order submission and fills are not simulated by this feed.

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
