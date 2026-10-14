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
import './App.css'

type ArchitectureId = 'pipe' | 'microkernel' | 'layered' | 'shared'
type SyncMode = 'mutex' | 'atomic' | 'hybrid'
type OptimizationMode = 'baseline' | 'optimized'
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
  layerCount: 5,
  syncMethod: 'hybrid' as SyncMode,
  lockContention: 38,
  optimizationMode: 'optimized' as OptimizationMode,
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

type Config = typeof defaultConfig

const clamp = (value: number, min: number, max: number) =>
  Math.min(max, Math.max(min, value))

function computeArchitectureMetrics(config: Config, architecture: ArchitectureId, simulationRun: number) {
  const feedPressure = config.exchangeFeeds * (config.marketEventsPerSecond / 100000)
  const burstFactor = 1 + config.volatility / 100 + (simulationRun % 5) * 0.018
  const networkLatency =
    config.exchangeRoundTripUs +
    config.networkJitterUs +
    config.ackDelayUs +
    config.packetLossPercent * 18
  const failurePenalty =
    config.exchangeFailureProbability * 1.8 +
    config.workerFailureProbability * 1.3 +
    config.pluginFailureProbability * 1.5 +
    config.queueOverflowProbability * 1.9 +
    config.feedDisconnectProbability * 1.5

  switch (architecture) {
    case 'pipe': {
      const queuePenalty = Math.max(0, config.marketEventsPerSecond / config.queueCapacity - 180) * 0.65
      const stagePenalty = (config.processingStages - 4) * 28
      const internalUs =
        config.averageProcessingUs * (1 + config.variabilityPercent / 120) +
        queuePenalty +
        stagePenalty +
        feedPressure * 8 +
        config.eventSizeKb * 22 +
        14 * burstFactor

      const endToEndUs = internalUs + networkLatency
      const throughput =
        (config.workerThreads * 145000) /
        Math.max(250, internalUs) *
        (1 - config.queueOverflowProbability / 100)
      const reliability = clamp(
        100 -
          config.stageFailureProbability * 5.8 -
          config.queueOverflowProbability * 4.2 -
          config.feedDisconnectProbability * 2.3 -
          failurePenalty,
        32,
        98,
      )
      const faultTolerance = clamp(
        100 - config.queueOverflowProbability * 12 - config.stageFailureProbability * 11,
        35,
        96,
      )
      const scalability = clamp(
        74 + config.workerThreads * 1.4 - config.exchangeFeeds * 0.9 + (config.optimizationMode === 'optimized' ? 10 : 0),
        38,
        95,
      )
      return {
        internalUs,
        endToEndUs,
        throughput,
        reliability,
        faultTolerance,
        scalability,
        score: clamp(
          52 + throughput / 2200 + reliability * 0.38 + scalability * 0.28 - endToEndUs / 38,
          0,
          100,
        ),
      }
    }
    case 'microkernel': {
      const dispatchPenalty =
        config.pluginCount * config.pluginOverheadUs +
        config.pluginCount * 18 +
        config.exchangeFeeds * 9
      const internalUs =
        config.averageProcessingUs * (1 + config.variabilityPercent / 140) +
        dispatchPenalty +
        feedPressure * 7 +
        config.workerFailureProbability * 8 +
        18 * burstFactor

      const endToEndUs = internalUs + networkLatency + config.pluginFailureProbability * 4
      const throughput =
        (config.workerThreads * 130000) /
        Math.max(260, internalUs) *
        (1 - config.pluginFailureProbability / 100)
      const reliability = clamp(
        100 -
          config.pluginFailureProbability * 6.2 -
          config.workerFailureProbability * 5.8 -
          config.exchangeFailureProbability * 2.6 -
          failurePenalty * 0.5,
        36,
        97,
      )
      const faultTolerance = clamp(
        100 - config.pluginFailureProbability * 13 - config.workerFailureProbability * 11,
        40,
        96,
      )
      const scalability = clamp(
        78 + config.pluginCount * 1.2 - config.exchangeFeeds * 0.3 + (config.optimizationMode === 'optimized' ? 7 : 0),
        44,
        96,
      )
      return {
        internalUs,
        endToEndUs,
        throughput,
        reliability,
        faultTolerance,
        scalability,
        score: clamp(
          54 + throughput / 2100 + reliability * 0.34 + scalability * 0.3 - endToEndUs / 42,
          0,
          100,
        ),
      }
    }
    case 'layered': {
      const serializationPenalty = config.layerCount * 30 + config.layerCount * config.eventSizeKb * 6
      const validationPenalty = config.layerCount * (config.averageProcessingUs * 0.12)
      const internalUs =
        config.averageProcessingUs * (1 + config.variabilityPercent / 110) +
        serializationPenalty +
        validationPenalty +
        (config.optimizationMode === 'optimized' ? -34 : 0) +
        feedPressure * 6 +
        config.eventSizeKb * 18

      const endToEndUs = internalUs + networkLatency + config.unavailabilityProbability * 7
      const throughput =
        (config.workerThreads * 112000) /
        Math.max(260, internalUs) *
        (1 - config.unavailabilityProbability / 100)
      const reliability = clamp(
        100 -
          config.exchangeFailureProbability * 2.7 -
          config.unavailabilityProbability * 5.5 -
          config.stageFailureProbability * 3.1 -
          failurePenalty * 0.45,
        30,
        96,
      )
      const faultTolerance = clamp(
        100 - config.unavailabilityProbability * 14 - config.workerFailureProbability * 10,
        35,
        95,
      )
      const scalability = clamp(
        70 + config.layerCount * 1.7 - config.exchangeFeeds * 0.8 + (config.optimizationMode === 'optimized' ? 18 : 0),
        40,
        98,
      )
      return {
        internalUs,
        endToEndUs,
        throughput,
        reliability,
        faultTolerance,
        scalability,
        score: clamp(
          50 + throughput / 2000 + reliability * 0.32 + scalability * 0.32 - endToEndUs / 46,
          0,
          100,
        ),
      }
    }
    case 'shared': {
      const contentionPenalty =
        config.lockContention * 5 +
        (config.syncMethod === 'mutex' ? 80 : config.syncMethod === 'atomic' ? 40 : 56) +
        config.workerThreads * 4
      const internalUs =
        config.averageProcessingUs * (1 + config.variabilityPercent / 150) +
        contentionPenalty +
        feedPressure * 5 +
        config.eventSizeKb * 26 +
        12 * burstFactor

      const endToEndUs = internalUs + networkLatency + config.lockContention * 2.6
      const throughput =
        (config.workerThreads * 138000) /
        Math.max(260, internalUs) *
        (1 - config.lockContention / 220)
      const reliability = clamp(
        100 -
          config.lockContention * 0.8 -
          config.workerFailureProbability * 4.5 -
          config.exchangeFailureProbability * 2.3 -
          failurePenalty * 0.5,
        32,
        97,
      )
      const faultTolerance = clamp(
        100 - config.lockContention * 0.9 - config.workerFailureProbability * 13,
        30,
        94,
      )
      const scalability = clamp(
        80 + config.workerThreads * 1.1 - config.lockContention * 0.5 - (config.syncMethod === 'mutex' ? 10 : 0),
        36,
        96,
      )
      return {
        internalUs,
        endToEndUs,
        throughput,
        reliability,
        faultTolerance,
        scalability,
        score: clamp(
          49 + throughput / 2050 + reliability * 0.31 + scalability * 0.3 - endToEndUs / 45,
          0,
          100,
        ),
      }
    }
    default:
      return {
        internalUs: 0,
        endToEndUs: 0,
        throughput: 0,
        reliability: 0,
        faultTolerance: 0,
        scalability: 0,
        score: 0,
      }
  }
}

function App() {
  const [config, setConfig] = useState<Config>(defaultConfig)
  const [simulationRun, setSimulationRun] = useState(1)
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
    setSimulationRun((value) => value + 1)
  }

  const architectureResults = (() => {
    const selected = architectureIds.filter((architecture) => simulationConfig.selectedArchitectures[architecture])

    if (selected.length === 0) {
      return architectureIds.map((architecture) => ({
        architecture,
        ...computeArchitectureMetrics(simulationConfig, architecture, simulationRun),
      }))
    }

    return selected.map((architecture) => ({
      architecture,
      ...computeArchitectureMetrics(simulationConfig, architecture, simulationRun),
    }))
  })()

  const sortedResults = [...architectureResults].sort((a, b) => b.score - a.score)
  const winner = sortedResults[0]
  const targetPassed = architectureResults.filter((result) => result.endToEndUs < 1000).length
  const latencyChart = architectureResults.map((result) => ({
    architecture: architectureMeta[result.architecture].short,
    latencyUs: Math.round(result.endToEndUs),
    internalUs: Math.round(result.internalUs),
    throughputK: Math.round(result.throughput / 1000),
    reliability: Math.round(result.reliability),
  }))

  const updateConfig = <K extends keyof Config>(key: K, value: Config[K]) => {
    setConfig((previous) => ({ ...previous, [key]: value }))
  }

  const toggleArchitecture = (architecture: ArchitectureId) => {
    setConfig((previous) => ({
      ...previous,
      selectedArchitectures: {
        ...previous.selectedArchitectures,
        [architecture]: !previous.selectedArchitectures[architecture],
      },
    }))
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
          <button type="button" className="primary-button" onClick={runSimulation}>
            Run simulation
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
          </div>
        </aside>

        <section className="content-panel">
          <div className="metric-grid">
            <MetricCard icon={<Gauge size={18} />} label="Best score" value={`${winner.score.toFixed(1)}/100`} tone="emerald" />
            <MetricCard icon={<TimerReset size={18} />} label="End-to-end latency" value={`${winner.endToEndUs.toFixed(0)} µs`} tone="amber" />
            <MetricCard icon={<BarChart3 size={18} />} label="Throughput" value={`${(winner.throughput / 1000).toFixed(0)}k evt/s`} tone="violet" />
            <MetricCard icon={<ShieldCheck size={18} />} label="Target pass" value={`${targetPassed}/${architectureResults.length}`} tone="rose" />
          </div>

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
                      setMarketStats({ tradeCount: 0, totalShares: 0, eventsPerSecond: 0 })
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
                <h3>Latency comparison</h3>
                <span>Internal + e2e latency</span>
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
              <span>{sortedResults.length} styles evaluated</span>
            </div>

            <table>
              <thead>
                <tr>
                  <th>Architecture</th>
                  <th>Internal</th>
                  <th>E2E</th>
                  <th>Throughput</th>
                  <th>Reliability</th>
                  <th>Score</th>
                </tr>
              </thead>
              <tbody>
                {sortedResults.map((result) => (
                  <tr key={result.architecture} className={winner.architecture === result.architecture ? 'highlight-row' : ''}>
                    <td>
                      <div className="architecture-cell">
                        <span className="dot" style={{ background: architectureMeta[result.architecture].accent }} />
                        {architectureMeta[result.architecture].label}
                      </div>
                    </td>
                    <td>{result.internalUs.toFixed(0)} µs</td>
                    <td>{result.endToEndUs.toFixed(0)} µs</td>
                    <td>{(result.throughput / 1000).toFixed(0)}k/s</td>
                    <td>{result.reliability.toFixed(0)}%</td>
                    <td>{result.score.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="insight-panel panel">
            <div className="panel-heading">
              <AlertTriangle size={18} />
              <h3>Architecture evaluation</h3>
            </div>
            <p>
              {winner.architecture === 'pipe' && (
                <>The pipe-and-filter model keeps independent feed processing parallel, though queue saturation and backpressure shorten the advantage as burstiness rises.</>
              )}
              {winner.architecture === 'microkernel' && (
                <>The microkernel approach keeps coordination minimal while isolating services, but plugin dispatch overhead becomes the main cost under heavier plugin fan-out.</>
              )}
              {winner.architecture === 'layered' && (
                <>The layered model remains easy to reason about and secure, but repeated validation and serialization across boundaries gradually reduce throughput and increase latency.</>
              )}
              {winner.architecture === 'shared' && (
                <>The shared-memory architecture minimizes inter-component transfer costs and can be very fast when contention stays low, yet mutex pressure and cache churn erode gains once lock intensity spikes.</>
              )}
            </p>
            <div className="summary-list">
              {sortedResults.map((result) => (
                <div key={result.architecture} className="summary-item">
                  <span className="summary-label">{architectureMeta[result.architecture].label}</span>
                  <span className="summary-value">{result.endToEndUs.toFixed(0)} µs · {result.reliability.toFixed(0)}% reliable</span>
                </div>
              ))}
            </div>
            <div className="recommendation">
              <span>Recommended focus</span>
              <strong>
                {winner.architecture === 'pipe' ? 'Keep pipeline parallelism but add adaptive backpressure.' : null}
                {winner.architecture === 'microkernel' ? 'Reduce plugin dispatch overhead and isolate high-risk plugins.' : null}
                {winner.architecture === 'layered' ? 'Use an optimized path across hot internal calls.' : null}
                {winner.architecture === 'shared' ? 'Prefer atomic or hybrid synchronization under heavy contention.' : null}
              </strong>
              <ArrowRight size={16} />
            </div>
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
