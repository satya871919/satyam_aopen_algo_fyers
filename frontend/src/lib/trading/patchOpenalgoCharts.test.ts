import { createChart } from 'openalgo-charts'
import { DrawingController, TREND_LINE } from 'openalgo-charts/draw'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { patchDrawingTimeline } from '../../../scripts/patch-openalgo-charts.mjs'

const original =
  'n.points=e.points.map(t=>({...t,time:t.time+i,price:o?t.price+r:this._host._screen.offsetPrice(t.price,e.paneIndex,s)}))'

describe('patchDrawingTimeline', () => {
  let chart: ReturnType<typeof createChart> | undefined
  let drawings: InstanceType<typeof DrawingController> | undefined

  beforeEach(() => {
    const context = new Proxy(
      {
        measureText: (text: string) => ({ width: text.length * 7 }),
        createLinearGradient: () => ({ addColorStop() {} }),
        getImageData: () => ({ data: new Uint8ClampedArray([0, 0, 0, 255]) }),
      },
      { get: (target, key) => target[key as keyof typeof target] ?? (() => {}) }
    )
    vi.spyOn(HTMLCanvasElement.prototype, 'getContext').mockReturnValue(
      context as unknown as CanvasRenderingContext2D
    )
  })

  afterEach(() => {
    drawings?.destroy()
    chart?.destroy()
    drawings = undefined
    chart = undefined
    vi.restoreAllMocks()
  })

  it('moves each anchor by the cursor delta on the logical chart timeline', () => {
    const patched = patchDrawingTimeline(original)

    expect(patched).toContain('dataLayer.timeToIndexFloat(this._dragStart.from.time+i)')
    expect(patched).toContain('dataLayer.indexToTimeFloat(')
    expect(patched).not.toContain('time:t.time+i')
  })

  it('is idempotent and refuses an unknown bundle shape', () => {
    const patched = patchDrawingTimeline(original)

    expect(patchDrawingTimeline(patched)).toBe(patched)
    expect(() => patchDrawingTimeline('unexpected bundle contents')).toThrow(
      'Expected one unpatched drawing drag expression; found 0'
    )
  })

  it('preserves drawing length when a drag crosses a session gap', () => {
    chart = createChart(document.createElement('div'), {
      shortcuts: false,
      timeNavigator: false,
      raf: {
        schedule: (callback) => {
          callback()
          return 1
        },
        cancel() {},
      },
    })
    chart.applySize(900, 500)
    const start = Date.UTC(2026, 0, 1, 9) / 1000
    const nextSession = start + 86_400
    chart.addSeries('candlestick').setData(
      [start, start + 60, start + 120, nextSession].map((time, index) => ({
        time,
        open: 100 + index,
        high: 101 + index,
        low: 99 + index,
        close: 100 + index,
      }))
    )
    drawings = new DrawingController(chart, { magnet: false })
    const line = drawings.add({
      tool: TREND_LINE.id,
      paneIndex: 0,
      points: [
        { time: start + 120, price: 102 },
        { time: nextSession, price: 103 },
      ],
      style: {},
    })
    const drag = Reflect.get(drawings, '_drag')
    if (!drag || typeof Reflect.get(drag, '_moveDrag') !== 'function') {
      throw new Error('Drawing drag handler is unavailable')
    }
    const originalDistance =
      chart.dataLayer.timeToIndexFloat(line.points[1]!.time) -
      chart.dataLayer.timeToIndexFloat(line.points[0]!.time)
    Reflect.set(drag, '_dragStart', {
      from: { time: start + 120, price: 102 },
      origin: { x: 100, y: 100 },
      anchor: null,
      items: [{ id: line.id, paneIndex: 0, points: line.points.map((point) => ({ ...point })) }],
    })
    Reflect.get(drag, '_moveDrag').call(
      drag,
      { time: nextSession, price: 102, paneIndex: 0, point: { x: 200, y: 100 } },
      line,
      null
    )

    const moved = drawings.get(line.id)
    expect(moved).toBeDefined()
    expect(
      chart.dataLayer.timeToIndexFloat(moved!.points[1]!.time) -
        chart.dataLayer.timeToIndexFloat(moved!.points[0]!.time)
    ).toBeCloseTo(originalDistance)
  })
})
