export type ArchitectureId = 'pipe' | 'microkernel' | 'layered' | 'shared'
export type SyncMode = 'mutex' | 'atomic' | 'hybrid'
export type OptimizationMode = 'baseline' | 'optimized'

export type SimulationConfig = {
  exchangeFeeds: number
  marketEventsPerSecond: number
  orderRequestsPerSecond: number
  simulationDuration: number
  workerThreads: number
  averageProcessingUs: number
  variabilityPercent: number
  volatility: number
  eventSizeKb: number
  queueCapacity: number
  processingStages: number
  pluginCount: number
  pluginOverheadUs: number
  pluginIsolation: number
  layerCount: number
  syncMethod: SyncMode
  lockContention: number
  optimizationMode: OptimizationMode
  exchangeRoundTripUs: number
  networkJitterUs: number
  ackDelayUs: number
  exchangeFailureProbability: number
  packetLossPercent: number
  stageFailureProbability: number
  workerFailureProbability: number
  pluginFailureProbability: number
  queueOverflowProbability: number
  feedDisconnectProbability: number
  unavailabilityProbability: number
  selectedArchitectures: Record<ArchitectureId, boolean>
}

export type MarketTradeSample = {
  symbol: string
  price: number
  size: number
}

export type ArchitectureSimulationResult = {
  architecture: ArchitectureId
  sampledOrders: number
  completedOrders: number
  rejectedOrders: number
  projectedOrders: number
  projectedCompletedOrders: number
  marketEventsDropped: number
  internalFailures: number
  queueDrops: number
  internalP50Us: number
  internalP95Us: number
  internalP99Us: number
  endToEndP50Us: number
  endToEndP95Us: number
  endToEndP99Us: number
  throughputOrdersPerSecond: number
  reliability: number
  faultTolerance: number
  scalability: number
  utilizationPercent: number
  capacityOrdersPerSecond: number
  score: number
  bottleneck: string
}

export type SimulationRunResult = {
  runNumber: number
  seed: number
  durationSeconds: number
  sampledEvents: number
  sampledOrders: number
  projectedEvents: number
  projectedOrders: number
  marketEventsPerSecond: number
  orderRequestsPerSecond: number
  liveTradeSamples: number
  liveMeanTradeSize: number
  liveVolatilityPercent: number
  elapsedMs: number
  architectures: ArchitectureSimulationResult[]
}

type StageSpec = {
  name: string
  serviceUs: number
  workers: number
}

type WorkloadEvent = {
  arrivalUs: number
  isOrder: boolean
}

const SAMPLE_LIMIT = 12000
const SAMPLE_FRACTION = 0.01
const TARGET_LATENCY_US = 1000

const clamp = (value: number, minimum: number, maximum: number) =>
  Math.min(maximum, Math.max(minimum, value))

function createRandom(seed: number) {
  let state = seed >>> 0
  return () => {
    state += 0x6d2b79f5
    let value = state
    value = Math.imul(value ^ (value >>> 15), value | 1)
    value ^= value + Math.imul(value ^ (value >>> 7), value | 61)
    return ((value ^ (value >>> 14)) >>> 0) / 4294967296
  }
}

function createSeed(config: SimulationConfig, runNumber: number) {
  const input = JSON.stringify({
    ...config,
    selectedArchitectures: Object.keys(config.selectedArchitectures)
      .sort()
      .map((key) => [key, config.selectedArchitectures[key as ArchitectureId]]),
    runNumber,
  })
  let hash = 2166136261
  for (let index = 0; index < input.length; index += 1) {
    hash ^= input.charCodeAt(index)
    hash = Math.imul(hash, 16777619)
  }
  return hash >>> 0
}

function percentile(values: number[], percentage: number) {
  if (values.length === 0) return 0
  const sorted = [...values].sort((left, right) => left - right)
  const index = Math.min(sorted.length - 1, Math.ceil((percentage / 100) * sorted.length) - 1)
  return sorted[Math.max(0, index)]
}

function createWorkload(config: SimulationConfig, count: number, seed: number): WorkloadEvent[] {
  const random = createRandom(seed)
  const totalRate = Math.max(1, config.marketEventsPerSecond + config.orderRequestsPerSecond)
  const orderShare = clamp(config.orderRequestsPerSecond / totalRate, 0, 1)
  const burstProbability = (clamp(config.volatility, 0, 100) / 100) * 0.15
  const ordinaryIntervalScale =
    burstProbability > 0
      ? (1 - burstProbability * 0.25) / (1 - burstProbability)
      : 1
  const events: WorkloadEvent[] = []
  let arrivalUs = 0

  for (let index = 0; index < count; index += 1) {
    const uniform = Math.max(Number.EPSILON, random())
    const burst = random() < burstProbability
    const burstScale = burst ? 0.25 : ordinaryIntervalScale
    arrivalUs += (-Math.log(uniform) * 1_000_000 * burstScale) / totalRate
    events.push({
      arrivalUs,
      isOrder: random() < orderShare,
    })
  }

  return events
}

function buildStages(
  config: SimulationConfig,
  architecture: ArchitectureId,
  meanTradeSize: number,
  liveVolatilityPercent: number,
): StageSpec[] {
  const eventSizeCost = config.eventSizeKb * 2
  const tradeSizeCost = Math.min(16, Math.log2(Math.max(1, meanTradeSize) + 1) * 1.5)
  const variabilityFactor = 1 + clamp(config.variabilityPercent, 0, 100) / 200
  const stages: StageSpec[] = []

  if (architecture === 'pipe') {
    const count = Math.max(2, Math.round(config.processingStages))
    const workersPerStage = Math.max(1, Math.floor(config.workerThreads / count))
    for (let index = 0; index < count; index += 1) {
      const workers = Math.max(
        1,
        workersPerStage + (index < config.workerThreads % count ? 1 : 0),
      )
      stages.push({
        name: `Filter ${index + 1}`,
        serviceUs:
          (config.averageProcessingUs / count) * variabilityFactor +
          4 +
          eventSizeCost / count +
          (index === 0 ? Math.log2(Math.max(1, config.exchangeFeeds)) * 3 : 0),
        workers,
      })
    }
    return stages
  }

  if (architecture === 'microkernel') {
    const count = Math.max(1, Math.round(config.pluginCount))
    for (let index = 0; index < count; index += 1) {
      stages.push({
        name: `Plugin ${index + 1}`,
        serviceUs:
          (config.averageProcessingUs / count) * variabilityFactor +
          config.pluginOverheadUs +
          8 +
          eventSizeCost / count +
          (index === 0 ? Math.log2(Math.max(1, config.exchangeFeeds)) * 3 : 0),
        workers: Math.max(1, Math.floor(config.workerThreads / count)),
      })
    }
    return stages
  }

  if (architecture === 'layered') {
    const count = Math.max(2, Math.round(config.layerCount))
    const boundaryUs =
      (config.optimizationMode === 'optimized' ? 4 : 18) +
      config.eventSizeKb * (config.optimizationMode === 'optimized' ? 1 : 4)
    for (let index = 0; index < count; index += 1) {
      stages.push({
        name: `Layer ${index + 1}`,
        serviceUs:
          (config.averageProcessingUs / count) * variabilityFactor +
          boundaryUs +
          eventSizeCost / count +
          (index === 0 ? Math.log2(Math.max(1, config.exchangeFeeds)) * 3 : 0),
        workers: Math.max(1, Math.floor(config.workerThreads / count)),
      })
    }
    return stages
  }

  const measuredMarketCost = liveVolatilityPercent * 0.18 + tradeSizeCost
  const syncMethodFactor =
    config.syncMethod === 'mutex' ? 1.5 : config.syncMethod === 'atomic' ? 0.65 : 1
  const computeUs =
    config.averageProcessingUs * 0.68 * variabilityFactor +
    Math.log2(Math.max(1, config.exchangeFeeds)) * 3
  const synchronizationUs =
    (5 + config.lockContention * 1.2) * syncMethodFactor + measuredMarketCost
  const orderBookUs = config.averageProcessingUs * 0.32 + eventSizeCost
  const synchronizationWorkers =
    config.syncMethod === 'mutex'
      ? 1
      : Math.max(
          1,
          Math.floor(
            config.workerThreads *
              (config.syncMethod === 'atomic'
                ? 1 - config.lockContention / 200
                : 1 - config.lockContention / 130),
          ),
        )

  return [
    { name: 'Shared compute', serviceUs: computeUs, workers: config.workerThreads },
    { name: 'Shared-state synchronization', serviceUs: synchronizationUs, workers: synchronizationWorkers },
    { name: 'Order book update', serviceUs: orderBookUs, workers: config.workerThreads },
  ]
}

function makeResult(
  config: SimulationConfig,
  architecture: ArchitectureId,
  events: WorkloadEvent[],
  runSeed: number,
  meanTradeSize: number,
  liveVolatilityPercent: number,
): ArchitectureSimulationResult {
  const random = createRandom(runSeed)
  const stages = buildStages(config, architecture, meanTradeSize, liveVolatilityPercent)
  const stageAvailability = stages.map((stage) => Array.from({ length: stage.workers }, () => 0))
  const stageServiceTotals = stages.map(() => 0)
  const internalLatencies: number[] = []
  const endToEndLatencies: number[] = []
  const totalRate = Math.max(1, config.marketEventsPerSecond + config.orderRequestsPerSecond)
  const simulatedWindowUs = Math.max(1, events[events.length - 1]?.arrivalUs ?? 1)
  const projectedOrders = Math.round(config.orderRequestsPerSecond * config.simulationDuration)
  const sampledOrders = events.reduce((count, event) => count + Number(event.isOrder), 0)
  let completedOrders = 0
  let rejectedOrders = 0
  let marketEventsDropped = 0
  let internalFailures = 0
  let queueDrops = 0

  const marketPathLength =
    architecture === 'pipe' || architecture === 'layered'
      ? Math.min(2, stages.length)
      : architecture === 'microkernel'
        ? Math.min(1, stages.length)
        : stages.length

  for (const event of events) {
    const isOrder = event.isOrder
    if (!isOrder && random() * 100 < config.feedDisconnectProbability) {
      marketEventsDropped += 1
      continue
    }

    let currentUs = event.arrivalUs
    let dropped = false
    const pathLength = isOrder ? stages.length : marketPathLength
    let serviceScale = isOrder ? 1 : 0.58

    if (random() * 100 < config.workerFailureProbability) {
      internalFailures += 1
      const isolationRecovery =
        architecture === 'microkernel'
          ? config.pluginIsolation / 100
          : architecture === 'pipe'
            ? 0.8
            : architecture === 'layered'
              ? 0.35
              : 0.5
      currentUs += 80 + random() * 220
      if (random() > isolationRecovery) {
        dropped = true
      }
    }

    for (let stageIndex = 0; stageIndex < pathLength && !dropped; stageIndex += 1) {
      const stage = stages[stageIndex]
      const availability = stageAvailability[stageIndex]
      let workerIndex = 0
      for (let index = 1; index < availability.length; index += 1) {
        if (availability[index] < availability[workerIndex]) workerIndex = index
      }

      const serviceUs =
        Math.max(0.1, stage.serviceUs * serviceScale * (0.8 + random() * 0.4))
      const waitUs = Math.max(0, availability[workerIndex] - currentUs)
      const queueLimitUs =
        (Math.max(1, config.queueCapacity) * stage.serviceUs) / Math.max(1, stage.workers)
      const physicalOverflow = waitUs > queueLimitUs
      const nearCapacity =
        waitUs > queueLimitUs * 0.7 &&
        random() * 100 < config.queueOverflowProbability

      if (physicalOverflow || nearCapacity) {
        queueDrops += 1
        dropped = true
        break
      }

      const serviceStartUs = currentUs + waitUs
      currentUs = serviceStartUs + serviceUs
      availability[workerIndex] = currentUs
      stageServiceTotals[stageIndex] += serviceUs

      if (random() * 100 < config.stageFailureProbability) {
        internalFailures += 1
        if (architecture === 'layered') {
          dropped = true
        } else if (architecture === 'microkernel') {
          if (random() * 100 > config.pluginIsolation) {
            dropped = true
          } else {
            currentUs += stage.serviceUs + config.pluginOverheadUs
            availability[workerIndex] = currentUs
          }
        } else {
          currentUs += stage.serviceUs * 0.6
          availability[workerIndex] = currentUs
        }
      }

      if (
        architecture === 'microkernel' &&
        random() * 100 < config.pluginFailureProbability
      ) {
        internalFailures += 1
        if (random() * 100 > config.pluginIsolation) {
          dropped = true
        } else {
          currentUs += config.pluginOverheadUs
          availability[workerIndex] = currentUs
        }
      }
    }

    const internalUs = Math.max(0, currentUs - event.arrivalUs)
    if (isOrder && !dropped) {
      internalLatencies.push(internalUs)
    }

    if (!isOrder) continue
    let endToEndUs = internalUs

    if (!dropped && random() * 100 < config.exchangeFailureProbability) {
      dropped = true
    }
    if (!dropped && random() * 100 < config.unavailabilityProbability) {
      dropped = true
    }

    if (!dropped) {
      const jitter = (random() * 2 - 1) * config.networkJitterUs
      endToEndUs += Math.max(0, config.exchangeRoundTripUs + jitter) + config.ackDelayUs

      let retransmissions = 0
      let packetLost = random() * 100 < config.packetLossPercent
      while (packetLost && retransmissions < 3) {
        retransmissions += 1
        const retryJitter = (random() * 2 - 1) * config.networkJitterUs
        endToEndUs +=
          Math.max(0, config.exchangeRoundTripUs + retryJitter) +
          config.ackDelayUs
        packetLost = random() * 100 < config.packetLossPercent
      }
      if (packetLost) {
        dropped = true
      }
    }

    if (dropped) {
      rejectedOrders += 1
    } else {
      completedOrders += 1
      endToEndLatencies.push(endToEndUs)
    }
  }

  const reliability =
    sampledOrders > 0 ? (completedOrders / sampledOrders) * 100 : 0
  const internalFailureRate =
    sampledOrders > 0 ? Math.min(1, internalFailures / sampledOrders) : 1
  const faultTolerance = clamp(100 - internalFailureRate * 100, 0, 100)
  const orderShare = config.orderRequestsPerSecond / totalRate
  const marketShare = 1 - orderShare
  const completionTimeUs = Math.max(
    simulatedWindowUs,
    ...stageAvailability.flat(),
  )
  const throughputOrdersPerSecond =
    completionTimeUs > 0
      ? (completedOrders / completionTimeUs) * 1_000_000
      : 0
  const stageOrderCapacities = stages.map((stage, index) => {
    const stageMarketShare = index < marketPathLength ? marketShare : 0
    const averageServiceUs =
      stage.serviceUs * (orderShare + stageMarketShare * 0.58)
    return (
      (stage.workers * 1_000_000 * orderShare) /
      Math.max(0.1, averageServiceUs)
    )
  })
  const modeledCapacityOrdersPerSecond = Math.min(...stageOrderCapacities)
  const capacityOrdersPerSecond = Math.max(
    modeledCapacityOrdersPerSecond,
    throughputOrdersPerSecond,
  )
  const scalability =
    config.orderRequestsPerSecond > 0
      ? clamp((capacityOrdersPerSecond / config.orderRequestsPerSecond) * 100, 0, 100)
      : 100
  const utilizationPercent = Math.max(
    ...stageServiceTotals.map(
      (serviceTotal, index) =>
        (serviceTotal /
          Math.max(1, stages[index].workers * simulatedWindowUs)) *
        100,
    ),
  )
  const projectedCompletedOrders = Math.round(
    Math.min(
      (projectedOrders * completedOrders) / Math.max(1, sampledOrders),
      throughputOrdersPerSecond * config.simulationDuration,
    ),
  )
  const internalP50Us = percentile(internalLatencies, 50)
  const internalP95Us = percentile(internalLatencies, 95)
  const internalP99Us = percentile(internalLatencies, 99)
  const endToEndP50Us = percentile(endToEndLatencies, 50)
  const endToEndP95Us = percentile(endToEndLatencies, 95)
  const endToEndP99Us = percentile(endToEndLatencies, 99)
  const latencyScore = clamp(
    100 * (1 - endToEndP99Us / (TARGET_LATENCY_US * 2)),
    0,
    100,
  )
  const throughputScore =
    config.orderRequestsPerSecond > 0
      ? clamp((throughputOrdersPerSecond / config.orderRequestsPerSecond) * 100, 0, 100)
      : 0
  const score =
    latencyScore * 0.35 +
    throughputScore * 0.25 +
    reliability * 0.2 +
    faultTolerance * 0.1 +
    scalability * 0.1

  let bottleneck = 'No single modeled bottleneck dominates this scenario.'
  if (queueDrops > 0 || utilizationPercent >= 100) {
    bottleneck = 'Processing capacity or bounded queues are saturated; add workers, reduce offered load, or increase queue capacity.'
  } else if (endToEndP99Us > TARGET_LATENCY_US) {
    bottleneck =
      endToEndP99Us - internalP99Us > internalP99Us
        ? 'Network round-trip, acknowledgment, jitter, or retransmission dominates tail latency.'
        : 'Internal processing and stage handoffs dominate tail latency.'
  } else if (internalFailures > 0) {
    bottleneck = 'Injected component failures and recovery overhead are reducing completion reliability.'
  }

  return {
    architecture,
    sampledOrders,
    completedOrders,
    rejectedOrders,
    projectedOrders,
    projectedCompletedOrders,
    marketEventsDropped,
    internalFailures,
    queueDrops,
    internalP50Us,
    internalP95Us,
    internalP99Us,
    endToEndP50Us,
    endToEndP95Us,
    endToEndP99Us,
    throughputOrdersPerSecond,
    reliability,
    faultTolerance,
    scalability,
    utilizationPercent,
    capacityOrdersPerSecond,
    score,
    bottleneck,
  }
}

export function simulateArchitectures(
  config: SimulationConfig,
  selectedArchitectures: ArchitectureId[],
  marketSamples: MarketTradeSample[],
  runNumber: number,
): SimulationRunResult {
  const startedAt = performance.now()
  const seed = createSeed(config, runNumber)
  const rate = Math.max(1, config.marketEventsPerSecond + config.orderRequestsPerSecond)
  const projectedEvents = Math.round(rate * config.simulationDuration)
  const desiredSamples = Math.ceil(projectedEvents * SAMPLE_FRACTION)
  const sampleCount = Math.min(SAMPLE_LIMIT, Math.max(1000, desiredSamples))
  const events = createWorkload(config, sampleCount, seed ^ 0x9e3779b9)
  const positiveSizes = marketSamples
    .map((sample) => sample.size)
    .filter((size) => Number.isFinite(size) && size > 0)
  const meanTradeSize =
    positiveSizes.length > 0
      ? positiveSizes.reduce((sum, size) => sum + size, 0) / positiveSizes.length
      : 1
  const returnsBySymbol = new Map<string, number[]>()

  for (let index = marketSamples.length - 1; index > 0; index -= 1) {
    const current = marketSamples[index]
    const previous = marketSamples[index - 1]
    if (
      current.symbol !== previous.symbol ||
      !Number.isFinite(current.price) ||
      current.price <= 0 ||
      !Number.isFinite(previous.price)
    ) {
      continue
    }
    const returns = returnsBySymbol.get(current.symbol) ?? []
    returns.push(Math.abs((current.price - previous.price) / previous.price) * 100)
    returnsBySymbol.set(current.symbol, returns)
  }

  const allReturns = [...returnsBySymbol.values()].flat()
  const liveVolatilityPercent =
    allReturns.length > 0
      ? percentile(allReturns, 75)
      : 0
  const architectures =
    selectedArchitectures.length > 0
      ? selectedArchitectures
      : (['pipe', 'microkernel', 'layered', 'shared'] as ArchitectureId[])
  const results = architectures.map((architecture) => {
    const architectureSeed = [...architecture].reduce(
      (hash, character) => Math.imul(hash ^ character.charCodeAt(0), 16777619),
      seed,
    )
    return makeResult(
      config,
      architecture,
      events,
      architectureSeed,
      meanTradeSize,
      liveVolatilityPercent,
    )
  })

  return {
    runNumber,
    seed,
    durationSeconds: config.simulationDuration,
    sampledEvents: events.length,
    sampledOrders: events.reduce((count, event) => count + Number(event.isOrder), 0),
    projectedEvents,
    projectedOrders: Math.round(config.orderRequestsPerSecond * config.simulationDuration),
    marketEventsPerSecond: config.marketEventsPerSecond,
    orderRequestsPerSecond: config.orderRequestsPerSecond,
    liveTradeSamples: marketSamples.length,
    liveMeanTradeSize: meanTradeSize,
    liveVolatilityPercent,
    elapsedMs: performance.now() - startedAt,
    architectures: results,
  }
}
