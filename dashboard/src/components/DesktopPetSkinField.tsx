import { useEffect, useRef, useState } from 'react'
import type { FieldHookComponentProps } from '@/lib/field-hooks'
import { backendApi } from '@/lib/http'
import { Button } from '@/components/ui/button'

interface Frame { file: string; rect: [number, number, number, number]; duration_ms: number }
interface Skin {
  id: string; name: string; version: string
  canvas: { width: number; height: number }
  animations: Record<string, { sequence?: Frame[]; duration_ms: number; motion?: string }>
  resources?: Record<string, string>
}
const root = '/api/webui/desktop-pet/skins'

export function DesktopPetSkinField({ value, onChange }: FieldHookComponentProps) {
  const [skins, setSkins] = useState<Skin[]>([])
  const [preview, setPreview] = useState<Skin | null>(null)
  const [animation, setAnimation] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const canvas = useRef<HTMLCanvasElement>(null)
  const selected = typeof value === 'string' ? value : 'builtin'
  async function refresh() {
    const result = await backendApi.get<{ skins: Skin[]; errors: {package: string; error: string}[] }>(root)
    setSkins(result.skins)
    setError(result.errors.map(item => `${item.package}: ${item.error}`).join('；'))
  }
  useEffect(() => { void refresh().catch(e => setError(String(e))) }, [])
  useEffect(() => {
    const controller = new AbortController()
    setPreview(null)
    void backendApi.get<Skin>(`${root}/${encodeURIComponent(selected)}`, { signal: controller.signal }).then(skin => {
      setPreview(skin); setAnimation(Object.keys(skin.animations)[0] || '')
    }).catch(e => { if (!controller.signal.aborted) setError(String(e)) })
    return () => controller.abort()
  }, [selected])
  useEffect(() => {
    if (!preview || preview.id === 'builtin') return
    let disposed = false, timer = 0
    const frames = preview.animations[animation]?.sequence
    if (!frames?.length) return
    const images: Record<string, HTMLImageElement> = {}
    void Promise.all(Object.entries(preview.resources || {}).map(async ([name, src]) => {
      const image = new Image(); image.src = src; await image.decode(); images[name] = image
    })).then(() => {
      if (disposed) return
      const started = performance.now()
      function draw(now: number) {
        if (disposed || !preview || !frames) return
        const element = canvas.current, context = element?.getContext('2d')
        if (!element || !context) return
        let elapsed = (now - started) % preview.animations[animation].duration_ms
        let frame = frames[0]
        for (const next of frames) { frame = next; if (elapsed < next.duration_ms) break; elapsed -= next.duration_ms }
        context.clearRect(0, 0, element.width, element.height)
        const [x, y, w, h] = frame.rect
        const motion = preview.animations[animation].motion
        const phase = (now - started) / Math.max(600, preview.animations[animation].duration_ms) * Math.PI * 2
        context.save()
        context.translate(element.width / 2, element.height - 4)
        if (motion === 'bob') context.translate(0, -2 - 2 * Math.sin(phase))
        if (motion === 'hop') context.translate(0, -Math.abs(Math.sin(phase)) * 8)
        if (motion === 'sway') context.rotate(Math.sin(phase) * .035)
        if (motion === 'shake') context.translate(Math.sin((now - started) / 40) * 3, 0)
        if (motion === 'pulse') context.scale(1 + Math.sin(phase) * .025, 1 - Math.sin(phase) * .025)
        context.translate(-element.width / 2, -element.height + 4)
        const scale = Math.min((element.width - 8) / w, (element.height - 12) / h)
        context.drawImage(images[frame.file], x, y, w, h, (element.width - w * scale) / 2, element.height - 4 - h * scale, w * scale, h * scale)
        context.restore()
        timer = requestAnimationFrame(draw)
      }
      timer = requestAnimationFrame(draw)
    }).catch(e => { if (!disposed) setError(String(e)) })
    return () => { disposed = true; cancelAnimationFrame(timer) }
  }, [preview, animation])
  return <div className="space-y-3">
    <select aria-label="当前皮肤" className="w-full rounded-md border bg-background p-2" value={selected} onChange={e => onChange?.(e.target.value)}>
      {!skins.some(s => s.id === selected) && <option value={selected}>{selected}</option>}
      {skins.map(s => <option key={s.id} value={s.id}>{s.name} · {s.version}</option>)}
    </select>
    <p className="text-xs text-muted-foreground">选择后保存配置生效。下方仅预览，不打断桌宠。</p>
    {preview?.id !== 'builtin' && preview && <div className="flex items-center gap-3 rounded-lg border p-3">
      <canvas ref={canvas} width={220} height={240} className="h-48 w-44 object-contain" aria-label="皮肤动画预览" />
      <div className="flex flex-wrap gap-2">{Object.keys(preview.animations).map(name => <Button key={name} type="button" variant={name === animation ? 'default' : 'outline'} size="sm" onClick={() => setAnimation(name)}>{name}</Button>)}</div>
    </div>}
    {preview?.id === 'builtin' && <p className="text-sm text-muted-foreground">内置 SVG 麦麦：浮动、眨眼、思考、说话与开心动作。</p>}
    <label className="block text-sm">导入皮肤 ZIP
      <input type="file" accept=".zip" disabled={busy} className="mt-1 block w-full text-sm" onChange={async e => {
        const file = e.target.files?.[0]; e.target.value = ''; if (!file) return
        if (file.size > 32 * 1024 * 1024) { setError('皮肤包不能超过 32 MiB'); return }
        setBusy(true); setError('')
        try { const skin = await backendApi.post<Skin>(root, { body: file }); await refresh(); onChange?.(skin.id) }
        catch (error) { setError(String(error)) }
        finally { setBusy(false) }
      }} />
    </label>
    {busy && <p className="text-sm">正在校验并安装…</p>}
    {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
  </div>
}
