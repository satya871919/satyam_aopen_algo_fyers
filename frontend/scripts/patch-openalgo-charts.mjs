import { readFile, writeFile } from 'node:fs/promises'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { dirname, resolve } from 'node:path'

const expectedVersion = '2.6.7'
const original = 'n.points=e.points.map(t=>({...t,time:t.time+i,price:o?t.price+r:this._host._screen.offsetPrice(t.price,e.paneIndex,s)}))'
const corrected = 'n.points=e.points.map(a=>({...a,time:this._host._chart.dataLayer.indexToTimeFloat(this._host._chart.dataLayer.timeToIndexFloat(a.time)+this._host._chart.dataLayer.timeToIndexFloat(this._dragStart.from.time+i)-this._host._chart.dataLayer.timeToIndexFloat(this._dragStart.from.time)),price:o?a.price+r:this._host._screen.offsetPrice(a.price,e.paneIndex,s)}))'

export function patchDrawingTimeline(source) {
  if (source.includes(corrected)) return source
  const count = source.split(original).length - 1
  if (count !== 1) {
    throw new Error(`Expected one unpatched drawing drag expression; found ${count}`)
  }
  return source.replace(original, corrected)
}

async function applyPatch() {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), '..')
  const packagePath = resolve(root, 'node_modules/openalgo-charts/package.json')
  const bundlePath = resolve(root, 'node_modules/openalgo-charts/dist/openalgo-charts.draw.mjs')
  const installed = JSON.parse(await readFile(packagePath, 'utf8'))
  if (installed.version !== expectedVersion) {
    throw new Error(`Drawing drag patch supports openalgo-charts ${expectedVersion}; found ${installed.version}`)
  }
  const source = await readFile(bundlePath, 'utf8')
  const patched = patchDrawingTimeline(source)
  if (patched !== source) await writeFile(bundlePath, patched)
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  await applyPatch()
}
