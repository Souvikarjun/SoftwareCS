import { createServer } from 'node:http'
import { WebSocket, WebSocketServer } from 'ws'

const port = Number(process.env.MARKET_PROXY_PORT || 8787)
const apiKey = process.env.ALPACA_API_KEY
const apiSecret = process.env.ALPACA_API_SECRET
const feedUrl = 'wss://stream.data.alpaca.markets/v2/iex'

const sendJson = (response, statusCode, body) => {
  response.writeHead(statusCode, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
  })
  response.end(JSON.stringify(body))
}

const server = createServer((request, response) => {
  const pathname = new URL(request.url || '/', `http://${request.headers.host || 'localhost'}`).pathname

  if (request.method === 'GET' && pathname === '/api/market/status') {
    sendJson(response, 200, {
      provider: 'Alpaca IEX',
      configured: Boolean(apiKey && apiSecret),
      feed: 'iex',
      maxSymbols: 20,
    })
    return
  }

  sendJson(response, 404, { error: 'Not found' })
})

const webSocketServer = new WebSocketServer({ noServer: true })

server.on('upgrade', (request, socket, head) => {
  const pathname = new URL(request.url || '/', `http://${request.headers.host || 'localhost'}`).pathname

  if (pathname !== '/market-stream') {
    socket.destroy()
    return
  }

  webSocketServer.handleUpgrade(request, socket, head, (client) => {
    webSocketServer.emit('connection', client)
  })
})

webSocketServer.on('connection', (client) => {
  let upstream
  let closed = false
  let authenticated = false
  let providerError = false
  let symbols = ['AAPL', 'MSFT', 'NVDA']
  let subscribedSymbols = []

  const send = (message) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message))
    }
  }

  const applySubscription = (nextSymbols) => {
    const validated = [...new Set(
      nextSymbols
        .filter((symbol) => typeof symbol === 'string')
        .map((symbol) => symbol.trim().toUpperCase())
        .filter((symbol) => /^[A-Z][A-Z0-9.-]{0,9}$/.test(symbol)),
    )].slice(0, 20)

    if (validated.length === 0) {
      send({ type: 'error', message: 'Enter one or more valid ticker symbols.' })
      return
    }

    symbols = validated
    if (authenticated && upstream?.readyState === WebSocket.OPEN) {
      const removed = subscribedSymbols.filter((symbol) => !symbols.includes(symbol))
      if (removed.length > 0) {
        upstream.send(JSON.stringify({ action: 'unsubscribe', trades: removed }))
      }
      upstream.send(JSON.stringify({ action: 'subscribe', trades: symbols }))
      subscribedSymbols = [...symbols]
      send({ type: 'status', status: 'connecting', message: `Subscribing to ${symbols.join(', ')}` })
    }
  }

  const connectUpstream = () => {
    if (closed || !apiKey || !apiSecret) {
      send({
        type: 'error',
        message: 'Set ALPACA_API_KEY and ALPACA_API_SECRET in .env, then restart the dev server.',
      })
      return
    }

    authenticated = false
    providerError = false
    send({ type: 'status', status: 'connecting', message: 'Connecting to Alpaca IEX…' })
    upstream = new WebSocket(feedUrl)

    upstream.on('open', () => {
      upstream.send(JSON.stringify({
        action: 'auth',
        key: apiKey,
        secret: apiSecret,
      }))
    })

    upstream.on('message', (raw) => {
      let messages
      try {
        messages = JSON.parse(raw.toString())
      } catch {
        send({ type: 'error', message: 'Received an unreadable message from the market data provider.' })
        return
      }

      for (const message of Array.isArray(messages) ? messages : [messages]) {
        if (message.T === 'success' && message.msg === 'authenticated') {
          authenticated = true
          subscribedSymbols = [...symbols]
          upstream.send(JSON.stringify({ action: 'subscribe', trades: symbols }))
          continue
        }

        if (message.T === 'success' && message.msg === 'subscribed') {
          send({
            type: 'status',
            status: 'connected',
            message: 'Live trade stream connected',
            symbols,
            feed: 'Alpaca IEX',
          })
          continue
        }

        if (message.T === 'error') {
          providerError = true
          send({ type: 'error', message: message.msg || 'Market data provider rejected the connection.' })
          upstream.close()
          continue
        }

        if (
          message.T === 't' &&
          typeof message.S === 'string' &&
          Number.isFinite(Number(message.p)) &&
          Number.isFinite(Number(message.s))
        ) {
          send({
            type: 'trade',
            trade: {
              id: `${message.S}-${message.t}-${message.i ?? ''}`,
              symbol: message.S,
              price: Number(message.p),
              size: Number(message.s),
              exchange: message.x || 'IEX',
              timestamp: message.t,
            },
          })
        }
      }
    })

    upstream.on('error', (error) => {
      send({ type: 'error', message: `Market data connection error: ${error.message}` })
    })

    upstream.on('close', () => {
      authenticated = false
      if (closed) return

      if (!providerError) {
        send({ type: 'status', status: 'disconnected', message: 'Feed disconnected; retrying in 3 seconds.' })
        setTimeout(connectUpstream, 3000)
      }
    })
  }

  client.on('message', (raw) => {
    let message
    try {
      message = JSON.parse(raw.toString())
    } catch {
      send({ type: 'error', message: 'Invalid stream control message.' })
      return
    }

    if (message.type === 'subscribe' && Array.isArray(message.symbols)) {
      applySubscription(message.symbols)
    }
  })

  client.on('close', () => {
    closed = true
    if (upstream && upstream.readyState < WebSocket.CLOSING) upstream.close()
  })

  connectUpstream()
})

server.listen(port, '127.0.0.1', () => {
  console.log(`Market data relay listening at http://127.0.0.1:${port}`)
})
