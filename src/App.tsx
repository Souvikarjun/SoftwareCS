import { motion } from 'framer-motion'
import {
  AlertTriangle,
  ArrowRight,
  BarChart3,
  Gauge,
  Radio,
  ShieldCheck,
  SlidersHorizontal,
  TimerReset,
} from 'lucide-react'
import { useEffect, useRef, useState } from 'react'
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import {
  simulateArchitectures,
  type ArchitectureId,
  type MarketTradeSample,
  type SimulationConfig,
  type SimulationRunResult,
} from './simulation'
import './App.css'

type MarketStatus = 'idle' | 'connecting' | 'connected' | 'disconnected' | 'error'

type LiveTrade = {
  id: string
  symbol: string
  price: number
  size: number
  exchange: string
  timestamp: string
}

type MarketStats = {
  tradeCount: number
  totalShares: number
  eventsPerSecond: number
  averageTradeSize: number
}

const architectureIds: ArchitectureId[] = ['pipe', 'microkernel', 'layered', 'shared']

const architectureMeta: Record<
  ArchitectureId,
  { label: string; short: string; accent: string; summary: string }
> = {
  pipe: {
    label: 'Pipe-and-Filter',
    short: 'Pipe',
    accent: '#5eead4',
    summary:
      'Very modular and naturally parallel across independent market feeds, but queueing adds latency under burst pressure.',
  },
  microkernel: {
    label: 'Microkernel',
    short: 'Kernel',
    accent: '#a78bfa',
    summary:
      'Minimal core with pluggable services offers strong isolation and recoverability, at the cost of dispatch overhead.',
  },
  layered: {
    label: 'Layered',
    short: 'Layered',
    accent: '#fbbf24',
    summary:
      'Clean separation of concerns helps safety and testability, but each boundary adds serialization and validation cost.',
  },
  shared: {
    label: 'Shared Memory',
    short: 'Shared',
    accent: '#fca5a5',
    summary:
      'Fast when contention is low, but synchronization and cache churn can dominate under heavy concurrent workloads.',
  },
}

const defaultConfig = {
  exchangeFeeds: 8,
  marketEventsPerSecond: 185000,
  orderRequestsPerSecond: 58000,
  simulationDuration: 30,
  workerThreads: 12,
  averageProcessingUs: 270,
  variabilityPercent: 24,
  volatility: 65,
  eventSizeKb: 1.8,
  queueCapacity: 420,
  processingStages: 6,
  pluginCount: 5,
  pluginOverheadUs: 72,
  pluginIsolation: 70,
  layerCount: 5,
  syncMethod: 'hybrid' as const,
  lockContention: 38,
  optimizationMode: 'optimized' as const,
  exchangeRoundTripUs: 160,
  networkJitterUs: 42,
  ackDelayUs: 85,
  exchangeFailureProbability: 1.8,
  packetLossPercent: 0.9,
  stageFailureProbability: 2.3,
  workerFailureProbability: 1.5,
  pluginFailureProbability: 2.7,
  queueOverflowProbability: 3.5,
  feedDisconnectProbability: 2.8,
  unavailabilityProbability: 2.1,
  selectedArchitectures: {
    pipe: true,
    microkernel: true,
    layered: true,
    shared: true,
  } as Record<ArchitectureId, boolean>,
}

type Config = SimulationConfig

function App() {
  const [config, setConfig] = useState<Config>(defaultConfig)
  const [simulationRun, setSimulationRun] = useState(0)
  const [simulationResult, setSimulationResult] = useState<SimulationRunResult | null>(null)
  const [isSimulationRunning, setIsSimulationRunning] = useState(false)
  const [simulationError, setSimulationError] = useState('')
  const [relayReady, setRelayReady] = useState(false)
  const [marketConfigured, setMarketConfigured] = useState(false)
  const [relayError, setRelayError] = useState('')
  const [marketStatus, setMarketStatus] = useState<MarketStatus>('idle')
  const [marketMessage, setMarketMessage] = useState('')
  const [isStreaming, setIsStreaming] = useState(false)
  const [symbolsInput, setSymbolsInput] = useState('AAPL, MSFT, NVDA')
  const [subscribedSymbols, setSubscribedSymbols] = useState<string[]>([])
  const [liveTrades, setLiveTrades] = useState<LiveTrade[]>([])
  const [marketStats, setMarketStats] = useState<MarketStats>({
    tradeCount: 0,
    totalShares: 0,
    eventsPerSecond: 0,
    averageTradeSize: 0,
  })
  const marketArrivalTimes = useRef<number[]>([])
  const streamStartedAt = useRef(0)

  useEffect(() => {
    let active = true

    fetch('/api/market/status')
      .then((response) => {
        if (!response.ok) throw new Error(`Market relay returned HTTP ${response.status}`)
        return response.json() as Promise<{ configured?: boolean }>
      })
      .then((result) => {
        if (!active) return
        setMarketConfigured(result.configured === true)
        setRelayReady(true)
      })
      .catch((error: unknown) => {
        if (!active) return
        setRelayError(error instanceof Error ? error.message : 'Could not reach the local market data relay.')
        setMarketStatus('error')
      })

    return () => {
      active = false
    }
  }, [])

  useEffect(() => {
    if (!isStreaming || !relayReady || !marketConfigured) return

    let stopped = false
    let socket: WebSocket | undefined
    let reconnectTimer: ReturnType<typeof setTimeout> | undefined
    const symbols = [...new Set(
      symbolsInput
        .split(/[,\s]+/)
        .map((symbol) => symbol.trim().toUpperCase())
        .filter(Boolean),
    )].slice(0, 20)

    const connect = () => {
      if (stopped) return
      setMarketStatus('connecting')
      setMarketMessage('Connecting to the local Alpaca IEX relay…')
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
      socket = new WebSocket(`${protocol}//${window.location.host}/market-stream`)

      socket.onopen = () => {
        streamStartedAt.current = Date.now()
        socket?.send(JSON.stringify({ type: 'subscribe', symbols }))
      }

      socket.onmessage = (event) => {
        try {
          const message = JSON.parse(String(event.data)) as {
            type?: string
            status?: string
            message?: string
            symbols?: string[]
            trade?: LiveTrade
          }

          if (message.type === 'status') {
            if (
              message.status === 'connected' ||
              message.status === 'connecting' ||
              message.status === 'disconnected'
            ) {
              setMarketStatus(message.status)
            }
            setMarketMessage(message.message || '')
            if (message.status === 'connected') {
              setSubscribedSymbols(message.symbols || symbols)
            }
            return
          }

          if (message.type === 'error') {
            setMarketStatus('error')
            setMarketMessage(message.message || 'Market data stream error.')
            return
          }

          if (
            message.type === 'trade' &&
            message.trade &&
            Number.isFinite(message.trade.price) &&
            Number.isFinite(message.trade.size)
          ) {
            const trade = message.trade
            const now = Date.now()
            const arrivals = [
              ...marketArrivalTimes.current.filter((time) => now - time <= 10000),
              now,
            ]
            marketArrivalTimes.current = arrivals
            setMarketStatus('connected')
            setMarketMessage('Live trade stream connected')
            const observationSeconds = Math.max(
              1,
              Math.min(10, (now - streamStartedAt.current) / 1000),
            )

            setLiveTrades((previous) => [trade, ...previous].slice(0, 40))
            setMarketStats((previous) => ({
              tradeCount: previous.tradeCount + 1,
              totalShares: previous.totalShares + trade.size,
              eventsPerSecond: arrivals.length / observationSeconds,
              averageTradeSize:
                (previous.averageTradeSize * previous.tradeCount + trade.size) /
                (previous.tradeCount + 1),
            }))
          }
        } catch {
          setMarketStatus('error')
          setMarketMessage('Could not parse an update from the local market data relay.')
        }
      }

      socket.onerror = () => {
        setMarketStatus('error')
        setMarketMessage('Could not connect to the local market data relay.')
      }

      socket.onclose = () => {
        if (stopped) return
        setMarketStatus('disconnected')
        setMarketMessage('Stream connection lost; reconnecting…')
        reconnectTimer = setTimeout(connect, 3000)
      }
    }

    connect()
    return () => {
      stopped = true
      if (reconnectTimer) clearTimeout(reconnectTimer)
      socket?.close()
    }
  }, [isStreaming, marketConfigured, relayReady, symbolsInput])

  useEffect(() => {
    if (marketStatus !== 'connected') return

    const timer = setInterval(() => {
      const now = Date.now()
      const arrivals = marketArrivalTimes.current.filter((time) => now - time <= 10000)
      marketArrivalTimes.current = arrivals
      const observationSeconds = Math.max(
        1,
        Math.min(10, (now - streamStartedAt.current) / 1000),
      )
      const eventsPerSecond = arrivals.length / observationSeconds
      setMarketStats((previous) =>
        previous.eventsPerSecond === eventsPerSecond
          ? previous
          : { ...previous, eventsPerSecond },
      )
    }, 1000)

    return () => clearInterval(timer)
  }, [marketStatus])

  const liveWorkloadActive = marketStatus === 'connected' && marketStats.tradeCount > 0
  const simulationConfig: Config = liveWorkloadActive
    ? {
        ...config,
        exchangeFeeds: 1,
        marketEventsPerSecond: Math.max(0, marketStats.eventsPerSecond),
      }
    : config

  const runSimulation = () => {
    if (isSimulationRunning) return

    const runNumber = simulationRun + 1
    const runConfig: Config = {
      ...simulationConfig,
      selectedArchitectures: { ...simulationConfig.selectedArchitectures },
    }
    const selected = architectureIds.filter(
      (architecture) => runConfig.selectedArchitectures[architecture],
    )
    const marketSamples: MarketTradeSample[] = liveWorkloadActive
      ? liveTrades.map(({ symbol, price, size }) => ({ symbol, price, size }))
      : []

    setSimulationRun(runNumber)
    setIsSimulationRunning(true)
    setSimulationError('')
    window.setTimeout(() => {
      try {
        setSimulationResult(
          simulateArchitectures(runConfig, selected, marketSamples, runNumber),
        )
      } catch (error) {
        setSimulationError(
          error instanceof Error ? error.message : 'The simulation could not be completed.',
        )
      } finally {
        setIsSimulationRunning(false)
      }
    }, 25)
  }

  const architectureResults = simulationResult?.architectures ?? []
  const sortedResults = [...architectureResults].sort((a, b) => b.score - a.score)
  const winner = sortedResults[0]
  const targetPassed = architectureResults.filter(
    (result) =>
      result.endToEndP99Us < 1000 &&
      result.throughputOrdersPerSecond >= (simulationResult?.orderRequestsPerSecond ?? 0) * 0.95 &&
      result.reliability >= 95,
  ).length
  const latencyChart = architectureResults.map((result) => ({
    architecture: architectureMeta[result.architecture].short,
    latencyUs: Math.round(result.endToEndP99Us),
    internalUs: Math.round(result.internalP99Us),
    throughputK: Math.round(result.throughputOrdersPerSecond / 1000),
    reliability: Math.round(result.reliability),
  }))

  const updateConfig = <K extends keyof Config>(key: K, value: Config[K]) => {
    setConfig((previous) => ({ ...previous, [key]: value }))
  }

  const toggleArchitecture = (architecture: ArchitectureId) => {
    setConfig((previous) => {
      const selectedCount = Object.values(previous.selectedArchitectures).filter(Boolean).length
      if (previous.selectedArchitectures[architecture] && selectedCount === 1) {
        return previous
      }
      return {
        ...previous,
        selectedArchitectures: {
          ...previous.selectedArchitectures,
          [architecture]: !previous.selectedArchitectures[architecture],
        },
      }
    })
  }

  return (
    <div className="app-shell">
      <header className="topbar panel">
        <div>
          <p className="eyebrow">Simulated estimates · live market input optional</p>
          <h1>HFT Architecture Lab</h1>
        </div>
        <div className="header-actions">
          <span className="status-pill">Target latency: &lt; 1000 µs</span>
          <button
            type="button"
            className="primary-button"
            onClick={runSimulation}
            disabled={isSimulationRunning}
          >
            {isSimulationRunning ? 'Simulating…' : 'Run simulation'}
          </button>
        </div>
      </header>

      <main className="dashboard">
        <aside className="control-panel panel">
          <div className="section-header">
            <SlidersHorizontal size={18} />
            <h2>Configuration</h2>
          </div>

          <div className="control-group">
            <h3>Workload</h3>
            <SliderField
              label="Exchange feeds"
              value={config.exchangeFeeds}
              min={1}
              max={16}
              step={1}
              onChange={(value) => updateConfig('exchangeFeeds', value)}
            />
            <SliderField
              label="Market events / s"
              value={config.marketEventsPerSecond}
              min={50000}
              max={500000}
              step={5000}
              onChange={(value) => updateConfig('marketEventsPerSecond', value)}
              suffix="evt/s"
            />
            <SliderField
              label="Order requests / s"
              value={config.orderRequestsPerSecond}
              min={10000}
              max={200000}
              step={2000}
              onChange={(value) => updateConfig('orderRequestsPerSecond', value)}
              suffix="ord/s"
            />
            <SliderField
              label="Simulation duration"
              value={config.simulationDuration}
              min={5}
              max={120}
              step={5}
              onChange={(value) => updateConfig('simulationDuration', value)}
              suffix="s"
            />
            <SliderField
              label="Worker threads"
              value={config.workerThreads}
              min={1}
              max={32}
              step={1}
              onChange={(value) => updateConfig('workerThreads', value)}
            />
            <SliderField
              label="Average processing"
              value={config.averageProcessingUs}
              min={80}
              max={800}
              step={10}
              onChange={(value) => updateConfig('averageProcessingUs', value)}
              suffix="µs"
            />
            <SliderField
              label="Processing variability"
              value={config.variabilityPercent}
              min={0}
              max={100}
              step={1}
              onChange={(value) => updateConfig('variabilityPercent', value)}
              suffix="%"
            />
            <SliderField
              label="Volatility"
              value={config.volatility}
              min={0}
              max={100}
              step={1}
              onChange={(value) => updateConfig('volatility', value)}
              suffix="%"
            />
            <SliderField
              label="Event size"
              value={config.eventSizeKb}
              min={0.2}
              max={8}
              step={0.1}
              onChange={(value) => updateConfig('eventSizeKb', value)}
              suffix="KB"
            />
          </div>

          <div className="control-group">
            <h3>Architecture</h3>
            <div className="toggle-row">
              {architectureIds.map((architecture) => (
                <button
                  key={architecture}
                  type="button"
                  className={
                    config.selectedArchitectures[architecture]
                      ? 'toggle-chip active'
                      : 'toggle-chip'
                  }
                  aria-pressed={config.selectedArchitectures[architecture]}
                  onClick={() => toggleArchitecture(architecture)}
                >
                  {architectureMeta[architecture].short}
                </button>
              ))}
            </div>
            <SliderField
              label="Queue capacity"
              value={config.queueCapacity}
              min={50}
              max={1200}
              step={10}
              onChange={(value) => updateConfig('queueCapacity', value)}
            />
            <SliderField
              label="Processing stages"
              value={config.processingStages}
              min={2}
              max={10}
              step={1}
              onChange={(value) => updateConfig('processingStages', value)}
            />
            <SliderField
              label="Plugin count"
              value={config.pluginCount}
              min={1}
              max={12}
              step={1}
              onChange={(value) => updateConfig('pluginCount', value)}
            />
            <SliderField
              label="Plugin dispatch overhead"
              value={config.pluginOverheadUs}
              min={0}
              max={200}
              step={2}
              onChange={(value) => updateConfig('pluginOverheadUs', value)}
              suffix="µs"
            />
            <SliderField
              label="Plugin isolation"
              value={config.pluginIsolation}
              min={0}
              max={100}
              step={1}
              onChange={(value) => updateConfig('pluginIsolation', value)}
              suffix="%"
            />
            <SliderField
              label="Layer count"
              value={config.layerCount}
              min={2}
              max={8}
              step={1}
              onChange={(value) => updateConfig('layerCount', value)}
            />
            <SliderField
              label="Lock contention"
              value={config.lockContention}
              min={0}
              max={100}
              step={1}
              onChange={(value) => updateConfig('lockContention', value)}
              suffix="%"
            />
            <label className="select-field">
              <span>Shared-state synchronization</span>
              <select
                value={config.syncMethod}
                onChange={(event) =>
                  updateConfig('syncMethod', event.target.value as Config['syncMethod'])
                }
              >
                <option value="mutex">Mutex</option>
                <option value="atomic">Atomic</option>
                <option value="hybrid">Hybrid</option>
              </select>
            </label>
            <label className="select-field">
              <span>Layer call path</span>
              <select
                value={config.optimizationMode}
                onChange={(event) =>
                  updateConfig(
                    'optimizationMode',
                    event.target.value as Config['optimizationMode'],
                  )
                }
              >
                <option value="optimized">Optimized internal calls</option>
                <option value="baseline">Baseline serialization</option>
              </select>
            </label>
          </div>

          <div className="control-group">
            <h3>Network & faults</h3>
            <SliderField
              label="Round-trip latency"
              value={config.exchangeRoundTripUs}
              min={20}
              max={500}
              step={10}
              onChange={(value) => updateConfig('exchangeRoundTripUs', value)}
              suffix="µs"
            />
            <SliderField
              label="Network jitter"
              value={config.networkJitterUs}
              min={0}
              max={120}
              step={2}
              onChange={(value) => updateConfig('networkJitterUs', value)}
              suffix="µs"
            />
            <SliderField
              label="Acknowledgment delay"
              value={config.ackDelayUs}
              min={0}
              max={300}
              step={5}
              onChange={(value) => updateConfig('ackDelayUs', value)}
              suffix="µs"
            />
            <SliderField
              label="Packet loss"
              value={config.packetLossPercent}
              min={0}
              max={10}
              step={0.1}
              onChange={(value) => updateConfig('packetLossPercent', value)}
              suffix="%"
            />
            <SliderField
              label="Exchange failure"
              value={config.exchangeFailureProbability}
              min={0}
              max={10}
              step={0.1}
              onChange={(value) => updateConfig('exchangeFailureProbability', value)}
              suffix="%"
            />
            <SliderField
              label="Queue overflow"
              value={config.queueOverflowProbability}
              min={0}
              max={12}
              step={0.1}
              onChange={(value) => updateConfig('queueOverflowProbability', value)}
              suffix="%"
            />
            <SliderField
              label="Plugin failure"
              value={config.pluginFailureProbability}
              min={0}
              max={12}
              step={0.1}
              onChange={(value) => updateConfig('pluginFailureProbability', value)}
              suffix="%"
            />
            <SliderField
              label="Processing-stage failure"
              value={config.stageFailureProbability}
              min={0}
              max={10}
              step={0.1}
              onChange={(value) => updateConfig('stageFailureProbability', value)}
              suffix="%"
            />
            <SliderField
              label="Worker failure"
              value={config.workerFailureProbability}
              min={0}
              max={10}
              step={0.1}
              onChange={(value) => updateConfig('workerFailureProbability', value)}
              suffix="%"
            />
            <SliderField
              label="Feed disconnect"
              value={config.feedDisconnectProbability}
              min={0}
              max={10}
              step={0.1}
              onChange={(value) => updateConfig('feedDisconnectProbability', value)}
              suffix="%"
            />
            <SliderField
              label="Exchange unavailability"
              value={config.unavailabilityProbability}
              min={0}
              max={10}
              step={0.1}
              onChange={(value) => updateConfig('unavailabilityProbability', value)}
              suffix="%"
            />
          </div>
        </aside>

        <section className="content-panel">
          <div className="metric-grid">
            <MetricCard icon={<Gauge size={18} />} label="Best score" value={winner ? `${winner.score.toFixed(1)}/100` : '—'} tone="emerald" />
            <MetricCard icon={<TimerReset size={18} />} label="E2E p99 latency" value={winner ? `${winner.endToEndP99Us.toFixed(0)} µs` : '—'} tone="amber" />
            <MetricCard icon={<BarChart3 size={18} />} label="Order throughput" value={winner ? `${(winner.throughputOrdersPerSecond / 1000).toFixed(1)}k ord/s` : '—'} tone="violet" />
            <MetricCard icon={<ShieldCheck size={18} />} label="SLO pass" value={simulationResult ? `${targetPassed}/${architectureResults.length}` : '—'} tone="rose" />
          </div>

          {simulationError && (
            <div className="simulation-error" role="alert">
              Simulation failed: {simulationError}
            </div>
          )}

          {simulationResult ? (
            <section className="simulation-report panel" aria-live="polite">
              <div className="simulation-report-header">
                <div>
                  <p className="eyebrow">Simulation run #{simulationResult.runNumber}</p>
                  <h2>Evaluation results</h2>
                </div>
                <span className="run-stamp">
                  Seed {simulationResult.seed} · {simulationResult.elapsedMs.toFixed(1)} ms compute
                </span>
              </div>
              <div className="run-facts">
                <span><strong>{simulationResult.sampledEvents.toLocaleString()}</strong> representative events processed</span>
                <span><strong>{simulationResult.sampledOrders.toLocaleString()}</strong> sampled order requests</span>
                <span><strong>{simulationResult.projectedEvents.toLocaleString()}</strong> events projected over {simulationResult.durationSeconds}s</span>
                <span><strong>{simulationResult.projectedOrders.toLocaleString()}</strong> projected order requests</span>
                <span>
                  <strong>{simulationResult.liveTradeSamples}</strong> live trades
                  {simulationResult.liveTradeSamples > 0
                    ? ` · ${simulationResult.liveMeanTradeSize.toFixed(1)} avg shares · ${simulationResult.liveVolatilityPercent.toFixed(3)}% observed move`
                    : ''}
                </span>
                <span>
                  Input rates: <strong>{simulationResult.marketEventsPerSecond.toFixed(1)} market/s</strong>
                  {' · '}
                  <strong>{simulationResult.orderRequestsPerSecond.toLocaleString()} orders/s</strong>
                </span>
              </div>
              {winner && (
                <div className="best-solution">
                  <div className="best-solution-heading">
                    <span className="best-solution-kicker">
                      {targetPassed > 0
                        ? 'Best solution meeting modeled SLOs'
                        : 'Highest-scoring available solution'}
                    </span>
                    <strong>{architectureMeta[winner.architecture].label}</strong>
                    <span className="score-badge">{winner.score.toFixed(1)} / 100</span>
                  </div>
                  <p>
                    {targetPassed > 0
                      ? 'This candidate met the modeled p99 latency, order-throughput, and completion-rate thresholds.'
                      : 'No evaluated candidate meets all modeled SLOs (p99 < 1 ms, at least 95% of requested order throughput, and at least 95% successful completion). This is the best score among the selected architectures, not a claim that the target is met.'}
                  </p>
                  <div className="winner-outcomes">
                    <span>p99 latency <strong>{winner.endToEndP99Us.toFixed(0)} µs</strong></span>
                    <span>Success rate <strong>{winner.reliability.toFixed(1)}%</strong></span>
                    <span>Estimated orders completed <strong>{winner.projectedCompletedOrders.toLocaleString()} / {winner.projectedOrders.toLocaleString()}</strong></span>
                    <span>Modeled capacity <strong>{Math.round(winner.capacityOrdersPerSecond).toLocaleString()} ord/s</strong></span>
                  </div>
                  <p className="run-losses">
                    In this sample: {winner.rejectedOrders.toLocaleString()} orders rejected, {winner.queueDrops.toLocaleString()} queue drops, {winner.internalFailures.toLocaleString()} internal fault events, and {winner.marketEventsDropped.toLocaleString()} feed events lost.
                  </p>
                  <p className="bottleneck-note"><strong>Primary bottleneck:</strong> {winner.bottleneck}</p>
                </div>
              )}
              <p className="simulation-caveat">
                Reproducible statistical model, not a hardware benchmark or measured execution. Results use the settings and live market snapshot captured when this run started.
              </p>
            </section>
          ) : (
            <section className="simulation-report panel run-prompt" aria-live="polite">
              <h2>{isSimulationRunning ? 'Running event simulation…' : 'Ready to evaluate'}</h2>
              <p>
                {isSimulationRunning
                  ? 'Processing a representative workload through the selected architecture models.'
                  : 'Set the workload and fault parameters, then run the simulation to process a reproducible event sample and rank the selected architectures.'}
              </p>
              <p>Each run evaluates queue waits, stage processing, network retries, injected faults, p99 latency, completion rate, and modeled capacity.</p>
            </section>
          )}

          <section className="live-feed-panel panel">
            <div className="live-feed-header">
              <div>
                <div className="panel-heading">
                  <Radio size={18} />
                  <h3>Live stock trades</h3>
                  <span className={`feed-status ${marketStatus}`}>
                    <span className="feed-status-dot" />
                    {marketStatus}
                  </span>
                </div>
                <p className="feed-description">
                  Alpaca IEX real-time trade prints. This is market data only—not a consolidated SIP feed or order execution.
                </p>
              </div>
              <div className="feed-controls">
                <label className="symbol-control">
                  <span>Symbols</span>
                  <input
                    value={symbolsInput}
                    onChange={(event) => setSymbolsInput(event.target.value)}
                    placeholder="AAPL, MSFT, NVDA"
                    aria-label="Comma-separated ticker symbols"
                    disabled={isStreaming}
                  />
                </label>
                <button
                  type="button"
                  className="feed-button"
                  disabled={!relayReady || !marketConfigured || (!isStreaming && symbolsInput.trim().length === 0)}
                  onClick={() => {
                    if (isStreaming) {
                      setIsStreaming(false)
                      setMarketStatus('idle')
                      setMarketMessage('Stream stopped.')
                    } else {
                      setLiveTrades([])
                      setMarketStats({
                        tradeCount: 0,
                        totalShares: 0,
                        eventsPerSecond: 0,
                        averageTradeSize: 0,
                      })
                      marketArrivalTimes.current = []
                      setIsStreaming(true)
                    }
                  }}
                >
                  {isStreaming ? 'Stop stream' : 'Start stream'}
                </button>
              </div>
            </div>

            {!relayReady && (
              <p className="feed-notice">
                {relayError || 'Checking the local market data relay…'}
              </p>
            )}
            {relayReady && !marketConfigured && (
              <p className="feed-notice">
                Add Alpaca API credentials to the local <code>.env</code> file and restart the app to enable live data.
              </p>
            )}
            {marketMessage && (isStreaming || marketStatus === 'error') && (
              <p className={`feed-message ${marketStatus === 'error' ? 'error' : ''}`}>{marketMessage}</p>
            )}

            <div className="live-stats">
              <div><span>Feed</span><strong>Alpaca IEX</strong></div>
              <div><span>Trades received</span><strong>{marketStats.tradeCount.toLocaleString()}</strong></div>
              <div><span>Observed rate</span><strong>{marketStats.eventsPerSecond.toFixed(1)} / s</strong></div>
              <div><span>Shares in tape</span><strong>{marketStats.totalShares.toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong></div>
              <div><span>Simulator input</span><strong>{liveWorkloadActive ? 'Live observed rate' : 'Configured workload'}</strong></div>
            </div>

            <div className="trade-tape-wrap">
              <table className="trade-tape">
                <thead>
                  <tr><th>Time (provider)</th><th>Symbol</th><th>Price</th><th>Size</th><th>Venue</th></tr>
                </thead>
                <tbody>
                  {liveTrades.length === 0 ? (
                    <tr><td colSpan={5} className="empty-tape">
                      {marketStatus === 'connected'
                        ? `Waiting for prints${subscribedSymbols.length ? ` for ${subscribedSymbols.join(', ')}` : ''}…`
                        : 'Start the stream to receive live market trades.'}
                    </td></tr>
                  ) : liveTrades.slice(0, 12).map((trade) => (
                    <tr key={trade.id}>
                      <td>{new Date(trade.timestamp).toLocaleTimeString()}</td>
                      <td className="tape-symbol">{trade.symbol}</td>
                      <td>${trade.price.toFixed(2)}</td>
                      <td>{trade.size.toLocaleString()}</td>
                      <td>{trade.exchange}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <p className="live-input-note">
              {liveWorkloadActive
                ? `Architecture estimates use the observed ${marketStats.eventsPerSecond.toFixed(1)} trade events/s from this stream. Processing and network costs remain configured model assumptions.`
                : 'Live trade prints are not used in the estimates until the stream is connected and trades arrive.'}
            </p>
          </section>

          <div className="chart-grid">
            <div className="chart-box panel">
              <div className="chart-title-row">
                <h3>P99 latency comparison</h3>
                <span>Successful orders · microseconds</span>
              </div>
              <ResponsiveContainer width="100%" height={260}>
                <BarChart data={latencyChart}>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" />
                  <XAxis dataKey="architecture" stroke="#cbd5e1" />
                  <YAxis stroke="#cbd5e1" tickFormatter={(value) => `${value}µs`} />
                  <Tooltip formatter={(value) => [`${value ?? 0} µs`, 'Latency']} />
                  <Legend />
                  <Bar dataKey="latencyUs" name="E2E" fill="#38bdf8" radius={[6, 6, 0, 0]} />
                  <Bar dataKey="internalUs" name="Internal" fill="#a78bfa" radius={[6, 6, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="chart-box panel">
              <div className="chart-title-row">
                <h3>Throughput & reliability</h3>
                <span>Higher is better</span>
              </div>
              <ResponsiveContainer width="100%" height={260}>
                <AreaChart data={latencyChart}>
                  <defs>
                    <linearGradient id="throughputFill" x1="0" x2="0" y1="0" y2="1">
                      <stop offset="5%" stopColor="#34d399" stopOpacity={0.8} />
                      <stop offset="95%" stopColor="#34d399" stopOpacity={0.15} />
                    </linearGradient>
                  </defs>
                  <CartesianGrid strokeDasharray="3 3" stroke="rgba(148,163,184,0.18)" />
                  <XAxis dataKey="architecture" stroke="#cbd5e1" />
                  <YAxis stroke="#cbd5e1" />
                  <Tooltip />
                  <Area type="monotone" dataKey="throughputK" name="Throughput (k/s)" stroke="#22c55e" fill="url(#throughputFill)" strokeWidth={2} />
                  <Area type="monotone" dataKey="reliability" name="Reliability %" stroke="#f59e0b" fillOpacity={0} strokeWidth={2} />
                </AreaChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="results-table panel">
            <div className="table-header">
              <h3>Architecture ranking</h3>
              <span>
                {simulationResult
                  ? `${sortedResults.length} styles · ranked by composite score`
                  : 'Run simulation to rank candidates'}
              </span>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Architecture</th>
                  <th>Internal p99</th>
                  <th>E2E p50</th>
                  <th>E2E p95</th>
                  <th>E2E p99</th>
                  <th>Orders / s</th>
                  <th>Success</th>
                  <th>Score</th>
                </tr>
              </thead>
              <tbody>
                {sortedResults.length === 0 && (
                  <tr><td colSpan={8} className="empty-tape">Results appear here after a simulation run.</td></tr>
                )}
                {sortedResults.map((result) => (
                  <tr key={result.architecture} className={winner.architecture === result.architecture ? 'highlight-row' : ''}>
                    <td>
                      <div className="architecture-cell">
                        <span className="dot" style={{ background: architectureMeta[result.architecture].accent }} />
                        {architectureMeta[result.architecture].label}
                      </div>
                    </td>
                    <td>{result.internalP99Us.toFixed(0)} µs</td>
                    <td>{result.endToEndP50Us.toFixed(0)} µs</td>
                    <td>{result.endToEndP95Us.toFixed(0)} µs</td>
                    <td>{result.endToEndP99Us.toFixed(0)} µs</td>
                    <td>{Math.round(result.throughputOrdersPerSecond).toLocaleString()}</td>
                    <td>{result.reliability.toFixed(1)}%</td>
                    <td>{result.score.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="insight-panel panel">
            <div className="panel-heading">
              <AlertTriangle size={18} />
              <h3>Why this architecture ranked highest</h3>
            </div>
            {winner ? (
              <>
                <p>{architectureMeta[winner.architecture].summary}</p>
                <div className="summary-list">
                  {sortedResults.map((result) => (
                    <div key={result.architecture} className="summary-item">
                      <span className="summary-label">{architectureMeta[result.architecture].label}</span>
                      <span className="summary-value">
                        p50 / p95 / p99 {result.endToEndP50Us.toFixed(0)} / {result.endToEndP95Us.toFixed(0)} / {result.endToEndP99Us.toFixed(0)} µs · {result.reliability.toFixed(1)}% completed · score {result.score.toFixed(1)}
                      </span>
                    </div>
                  ))}
                </div>
                <div className="recommendation">
                  <span>Next optimization</span>
                  <strong>{winner.bottleneck}</strong>
                  <ArrowRight size={16} />
                </div>
              </>
            ) : (
              <p>Run the simulator to compare results and get a scenario-specific architecture recommendation.</p>
            )}
          </div>
        </section>
      </main>
    </div>
  )
}

type SliderFieldProps = {
  label: string
  value: number
  min: number
  max: number
  step: number
  suffix?: string
  onChange: (value: number) => void
}

function SliderField({ label, value, min, max, step, suffix, onChange }: SliderFieldProps) {
  return (
    <label className="slider-field">
      <div className="slider-header">
        <span>{label}</span>
        <strong>
          {value}
          {suffix ?? ''}
        </strong>
      </div>
      <input
        className="range-input"
        type="range"
        min={min}
        max={max}
        step={step}
        value={value}
        onChange={(event) => onChange(Number(event.target.value))}
      />
    </label>
  )
}

type MetricCardProps = {
  icon: React.ReactNode
  label: string
  value: string
  tone: 'emerald' | 'amber' | 'violet' | 'rose'
}

function MetricCard({ icon, label, value, tone }: MetricCardProps) {
  return (
    <motion.div className={`metric-card ${tone}`} initial={{ opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }}>
      <div className="card-icon">{icon}</div>
      <div>
        <p>{label}</p>
        <strong>{value}</strong>
      </div>
    </motion.div>
  )
}

export default App
