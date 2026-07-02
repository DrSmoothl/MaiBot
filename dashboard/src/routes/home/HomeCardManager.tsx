import type { ReactNode } from 'react'
import {
  DndContext,
  PointerSensor,
  useSensor,
  useSensors,
  type DragEndEvent,
  type DragStartEvent,
  useDraggable,
} from '@dnd-kit/core'
import { CSS } from '@dnd-kit/utilities'
import { ExternalLink, GripVertical, Plus, RotateCcw, X } from 'lucide-react'
import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useTranslation } from 'react-i18next'
import ReactMarkdown from 'react-markdown'
import remarkGfm from 'remark-gfm'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import {
  Dialog,
  DialogBody,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import type { PluginHomeCard, PluginHomeCardContent, PluginHomeCardWidth } from '@/lib/plugin-api'
import { cn } from '@/lib/utils'

const HOME_CARD_LAYOUT_STORAGE_KEY = 'maibot-home-card-layout-v1'
const CANVAS_COLUMNS = 12
const CANVAS_GAP = 16
const CANVAS_ROW_HEIGHT = 64
const CANVAS_MIN_HEIGHT = 220

type HomeCardSource = 'builtin' | 'plugin'

export interface HomeCardDefinition {
  id: string
  title: string
  description?: string
  width?: PluginHomeCardWidth
  source: HomeCardSource
  render: () => ReactNode
}

interface HomeCardLayout {
  order: string[]
  hidden: string[]
  positions: Record<string, HomeCardPosition>
  zOrder: string[]
}

interface HomeCardPosition {
  x: number
  y: number
  z: number
}

interface HomeCardManagerProps {
  cards: HomeCardDefinition[]
  pluginCards: PluginHomeCard[]
  controlsPortalId?: string
}

function loadHomeCardLayout(): HomeCardLayout {
  if (typeof window === 'undefined') {
    return { order: [], hidden: [], positions: {}, zOrder: [] }
  }

  try {
    const parsed = JSON.parse(localStorage.getItem(HOME_CARD_LAYOUT_STORAGE_KEY) || '{}')
    return {
      order: Array.isArray(parsed.order) ? parsed.order.filter((item: unknown): item is string => typeof item === 'string') : [],
      hidden: Array.isArray(parsed.hidden) ? parsed.hidden.filter((item: unknown): item is string => typeof item === 'string') : [],
      positions: isPositionMap(parsed.positions) ? parsed.positions : {},
      zOrder: Array.isArray(parsed.zOrder) ? parsed.zOrder.filter((item: unknown): item is string => typeof item === 'string') : [],
    }
  } catch {
    return { order: [], hidden: [], positions: {}, zOrder: [] }
  }
}

function saveHomeCardLayout(layout: HomeCardLayout): void {
  localStorage.setItem(HOME_CARD_LAYOUT_STORAGE_KEY, JSON.stringify(layout))
}

function isPositionMap(value: unknown): value is Record<string, HomeCardPosition> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return false
  return Object.values(value).every((item) => {
    if (!item || typeof item !== 'object' || Array.isArray(item)) return false
    const position = item as HomeCardPosition
    return Number.isFinite(position.x) && Number.isFinite(position.y) && Number.isFinite(position.z)
  })
}

function sanitizeUrl(url: unknown): string {
  const value = String(url || '').trim()
  if (!value || value.startsWith('//')) return ''
  const lower = value.toLowerCase()
  if (value.startsWith('/') || lower.startsWith('http://') || lower.startsWith('https://') || lower.startsWith('mailto:')) {
    return value
  }
  return ''
}

function cardGridSize(width: PluginHomeCardWidth | undefined): { width: number; height: number } {
  switch (width) {
    case 'small':
      return { width: 3, height: 3 }
    case 'medium':
      return { width: 4, height: 3 }
    case 'large':
      return { width: 5, height: 3 }
    case 'full':
      return { width: CANVAS_COLUMNS, height: 5 }
    default:
      return { width: 4, height: 3 }
  }
}

function HomeMarkdown({ content }: { content: string }) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      urlTransform={(url) => sanitizeUrl(url)}
      components={{
        a({ children, href, ...props }) {
          const safeHref = sanitizeUrl(href)
          if (!safeHref) return <span>{children}</span>
          return (
            <a className="text-primary hover:underline" href={safeHref} target="_blank" rel="noopener noreferrer" {...props}>
              {children}
            </a>
          )
        },
        p({ children }) {
          return <p className="my-1.5 leading-relaxed">{children}</p>
        },
        ul({ children }) {
          return <ul className="my-2 list-inside list-disc space-y-1">{children}</ul>
        },
        ol({ children }) {
          return <ol className="my-2 list-inside list-decimal space-y-1">{children}</ol>
        },
        code({ children }) {
          return <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">{children}</code>
        },
      }}
    >
      {content}
    </ReactMarkdown>
  )
}

function getBlockText(block: Record<string, unknown>, keys: string[]): string {
  for (const key of keys) {
    const value = block[key]
    if (typeof value === 'string' && value.trim()) {
      return value
    }
  }
  return ''
}

function renderContentBlock(block: Record<string, unknown>, index: number): ReactNode {
  const type = String(block.type || 'text')
  if (type === 'markdown') {
    return <HomeMarkdown key={index} content={getBlockText(block, ['content', 'text', 'value'])} />
  }
  if (type === 'stat') {
    return (
      <div key={index} className="rounded-md border bg-muted/20 px-3 py-2">
        <div className="text-xs text-muted-foreground">{getBlockText(block, ['label', 'title'])}</div>
        <div className="mt-1 text-xl font-bold">{getBlockText(block, ['value', 'content'])}</div>
        {getBlockText(block, ['detail', 'description']) && (
          <div className="mt-1 text-xs text-muted-foreground">{getBlockText(block, ['detail', 'description'])}</div>
        )}
      </div>
    )
  }
  if (type === 'key_value') {
    const entries = block.entries && typeof block.entries === 'object' && !Array.isArray(block.entries)
      ? Object.entries(block.entries as Record<string, unknown>)
      : []
    return (
      <div key={index} className="space-y-1.5">
        {entries.map(([key, value]) => (
          <div key={key} className="flex items-center justify-between gap-3 text-sm">
            <span className="text-muted-foreground">{key}</span>
            <span className="min-w-0 truncate font-medium">{String(value || '')}</span>
          </div>
        ))}
      </div>
    )
  }
  if (type === 'list' && Array.isArray(block.items)) {
    return (
      <ul key={index} className="list-inside list-disc space-y-1 text-sm">
        {block.items.map((item, itemIndex) => (
          <li key={itemIndex}>{String(item || '')}</li>
        ))}
      </ul>
    )
  }
  if (type === 'actions' && Array.isArray(block.actions)) {
    return (
      <div key={index} className="flex flex-wrap gap-2">
        {block.actions.map((item, itemIndex) => {
          if (!item || typeof item !== 'object') return null
          const action = item as Record<string, unknown>
          const href = sanitizeUrl(action.url || action.href)
          if (!href) return null
          return (
            <Button key={itemIndex} variant="outline" size="sm" asChild>
              <a href={href} target={href.startsWith('/') ? undefined : '_blank'} rel={href.startsWith('/') ? undefined : 'noopener noreferrer'}>
                {getBlockText(action, ['label', 'title']) || href}
              </a>
            </Button>
          )
        })}
      </div>
    )
  }
  return <p key={index} className="text-sm leading-relaxed">{getBlockText(block, ['content', 'text', 'value'])}</p>
}

function PluginHomeCardView({ card }: { card: PluginHomeCard }) {
  const href = sanitizeUrl(card.link_url)
  const content = renderPluginContent(card.content)

  return (
    <Card className="h-full">
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between gap-3">
          <div className="min-w-0 space-y-1">
            <CardTitle className="truncate text-sm font-medium">{card.title}</CardTitle>
            {card.description && <CardDescription className="line-clamp-2">{card.description}</CardDescription>}
          </div>
          <Badge variant="outline" className="shrink-0 text-[10px]">插件</Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        {content}
        {href && (
          <Button variant="outline" size="sm" asChild className="w-full justify-start gap-2">
            <a href={href} target={href.startsWith('/') ? undefined : '_blank'} rel={href.startsWith('/') ? undefined : 'noopener noreferrer'}>
              {card.link_label || '打开'}
              {!href.startsWith('/') && <ExternalLink className="h-3.5 w-3.5" />}
            </a>
          </Button>
        )}
      </CardContent>
    </Card>
  )
}

function renderPluginContent(content: PluginHomeCardContent): ReactNode {
  if (typeof content === 'string') {
    return content.trim() ? <HomeMarkdown content={content} /> : <p className="text-sm text-muted-foreground">暂无内容</p>
  }
  if (Array.isArray(content)) {
    return <div className="space-y-3">{content.map(renderContentBlock)}</div>
  }
  if (content && typeof content === 'object') {
    return renderContentBlock(content, 0)
  }
  return <p className="text-sm text-muted-foreground">暂无内容</p>
}

function getDefaultCardPositions(cards: HomeCardDefinition[]): Record<string, HomeCardPosition> {
  const positions: Record<string, HomeCardPosition> = {}
  let x = 0
  let y = 0
  let rowHeight = 0

  cards.forEach((card, index) => {
    const size = cardGridSize(card.width)
    if (x > 0 && x + size.width > CANVAS_COLUMNS) {
      y += rowHeight
      x = 0
      rowHeight = 0
    }

    positions[card.id] = { x, y, z: index + 1 }
    x += size.width
    rowHeight = Math.max(rowHeight, size.height)
  })

  return positions
}

function clampCardPosition(position: HomeCardPosition, card: HomeCardDefinition): HomeCardPosition {
  const size = cardGridSize(card.width)
  return {
    x: Math.max(0, Math.min(CANVAS_COLUMNS - size.width, Math.round(position.x))),
    y: Math.max(0, Math.round(position.y)),
    z: Math.max(1, Math.round(position.z)),
  }
}

function positionsEqual(left: Record<string, HomeCardPosition>, right: Record<string, HomeCardPosition>): boolean {
  const leftKeys = Object.keys(left)
  const rightKeys = Object.keys(right)
  if (leftKeys.length !== rightKeys.length) return false
  return leftKeys.every((key) => {
    const leftPosition = left[key]
    const rightPosition = right[key]
    return Boolean(rightPosition)
      && leftPosition.x === rightPosition.x
      && leftPosition.y === rightPosition.y
      && leftPosition.z === rightPosition.z
  })
}

function stringArraysEqual(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((item, index) => item === right[index])
}

function FreeformHomeCard({
  card,
  columnWidth,
  editing,
  onHide,
  position,
  size,
}: {
  card: HomeCardDefinition
  columnWidth: number
  editing: boolean
  onHide: (id: string) => void
  position: HomeCardPosition
  size: { height: number; width: number }
}) {
  const { attributes, listeners, setNodeRef, transform, isDragging } = useDraggable({
    id: card.id,
    disabled: !editing,
  })
  const left = position.x * (columnWidth + CANVAS_GAP)
  const top = position.y * (CANVAS_ROW_HEIGHT + CANVAS_GAP)
  const width = size.width * columnWidth + (size.width - 1) * CANVAS_GAP
  const height = size.height * CANVAS_ROW_HEIGHT + (size.height - 1) * CANVAS_GAP
  const style = {
    height,
    left,
    transform: CSS.Translate.toString(transform),
    top,
    width,
    zIndex: isDragging ? 1000 : position.z,
  }

  return (
    <div
      ref={setNodeRef}
      data-home-card-canvas-item="true"
      style={style}
      className={cn('absolute min-w-0 overflow-hidden transition-[filter,opacity] duration-150', isDragging && 'opacity-90')}
    >
      {editing && (
        <div
          data-home-card-edit-overlay="true"
          aria-hidden="true"
          className="absolute inset-0 z-10 rounded-lg border border-primary/25 bg-white/20 shadow-[inset_0_1px_0_rgba(255,255,255,0.38),inset_0_0_0_1px_rgba(255,255,255,0.12)] backdrop-blur-md backdrop-saturate-150 dark:bg-black/20"
          style={{
            WebkitBackdropFilter: 'blur(10px) saturate(140%)',
            backdropFilter: 'blur(10px) saturate(140%)',
          }}
        />
      )}
      {editing && (
        <div className="absolute right-2 top-2 z-20 flex items-center gap-1 rounded-md border bg-background/95 p-1 shadow-sm backdrop-blur">
          <Tooltip>
            <TooltipTrigger asChild>
              <Button variant="ghost" size="icon" className="h-7 w-7 cursor-grab" {...attributes} {...listeners}>
                <GripVertical className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>拖拽排序</TooltipContent>
          </Tooltip>
          <Tooltip>
            <TooltipTrigger asChild>
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                onClick={() => onHide(card.id)}
              >
                <X className="h-4 w-4" />
              </Button>
            </TooltipTrigger>
            <TooltipContent>从首页隐藏</TooltipContent>
          </Tooltip>
        </div>
      )}
      <div
        aria-hidden={editing}
        className={cn(
          'h-full transition-[filter,opacity] duration-150',
          editing && 'pointer-events-none select-none blur-[2.5px] opacity-75'
        )}
        inert={editing}
      >
        {card.render()}
      </div>
    </div>
  )
}

export function HomeCardManager({ cards, pluginCards, controlsPortalId }: HomeCardManagerProps) {
  const { t } = useTranslation()
  const [layout, setLayout] = useState<HomeCardLayout>(loadHomeCardLayout)
  const [canvasWidth, setCanvasWidth] = useState(0)
  const [editing, setEditing] = useState(false)
  const [dialogOpen, setDialogOpen] = useState(false)
  const [controlsContainer, setControlsContainer] = useState<HTMLElement | null>(null)
  const canvasRef = useRef<HTMLDivElement | null>(null)

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 6 } })
  )

  const pluginDefinitions = useMemo<HomeCardDefinition[]>(
    () =>
      pluginCards.map((card) => ({
        id: card.id,
        title: card.title,
        description: card.description,
        width: card.width,
        source: 'plugin' as const,
        render: () => <PluginHomeCardView card={card} />,
      })),
    [pluginCards]
  )

  const allCards = useMemo(
    () => [...cards, ...pluginDefinitions],
    [cards, pluginDefinitions]
  )
  const cardMap = useMemo(() => new Map(allCards.map((card) => [card.id, card])), [allCards])
  const allCardIds = useMemo(() => allCards.map((card) => card.id), [allCards])
  const columnWidth = useMemo(
    () => Math.max(0, (canvasWidth - CANVAS_GAP * (CANVAS_COLUMNS - 1)) / CANVAS_COLUMNS),
    [canvasWidth]
  )

  const updateLayout = useCallback((updater: (current: HomeCardLayout) => HomeCardLayout) => {
    setLayout((current) => {
      const next = updater(current)
      saveHomeCardLayout(next)
      return next
    })
  }, [])

  useEffect(() => {
    updateLayout((current) => {
      const knownIds = new Set(allCardIds)
      const order = [...current.order.filter((id) => knownIds.has(id)), ...allCardIds.filter((id) => !current.order.includes(id))]
      const hidden = current.hidden.filter((id) => knownIds.has(id))
      const defaultPositions = getDefaultCardPositions(order.map((id) => cardMap.get(id)).filter((card): card is HomeCardDefinition => card !== undefined))
      const positions: Record<string, HomeCardPosition> = {}
      for (const id of order) {
        const card = cardMap.get(id)
        if (!card) continue
        positions[id] = clampCardPosition(current.positions[id] || defaultPositions[id], card)
      }
      const zOrder = [...current.zOrder.filter((id) => knownIds.has(id)), ...allCardIds.filter((id) => !current.zOrder.includes(id))]
      if (
        stringArraysEqual(order, current.order)
        && stringArraysEqual(hidden, current.hidden)
        && stringArraysEqual(zOrder, current.zOrder)
        && positionsEqual(positions, current.positions)
      ) {
        return current
      }
      return { order, hidden, positions, zOrder }
    })
  }, [allCardIds, cardMap, updateLayout])

  useEffect(() => {
    const canvasElement = canvasRef.current
    if (!canvasElement) return

    const updateCanvasWidth = () => setCanvasWidth(canvasElement.clientWidth)
    updateCanvasWidth()
    const observer = new ResizeObserver(updateCanvasWidth)
    observer.observe(canvasElement)
    return () => observer.disconnect()
  }, [])

  useEffect(() => {
    if (!controlsPortalId || typeof document === 'undefined') {
      setControlsContainer(null)
      return
    }
    setControlsContainer(document.getElementById(controlsPortalId))
  }, [controlsPortalId])

  const visibleCards = useMemo(
    () =>
      layout.order
        .map((id) => cardMap.get(id))
        .filter((card): card is HomeCardDefinition => card !== undefined && !layout.hidden.includes(card.id)),
    [cardMap, layout.hidden, layout.order]
  )
  const hiddenCards = useMemo(
    () =>
      layout.hidden
        .map((id) => cardMap.get(id))
        .filter((card): card is HomeCardDefinition => card !== undefined),
    [cardMap, layout.hidden]
  )

  const bringCardToFront = useCallback((id: string) => {
    updateLayout((current) => {
      const card = cardMap.get(id)
      const position = current.positions[id]
      if (!card || !position) return current
      const maxZ = Math.max(0, ...Object.values(current.positions).map((item) => item.z))
      const zOrder = [...current.zOrder.filter((item) => item !== id), id]
      return {
        ...current,
        positions: {
          ...current.positions,
          [id]: { ...position, z: maxZ + 1 },
        },
        zOrder,
      }
    })
  }, [cardMap, updateLayout])

  const handleDragStart = useCallback((event: DragStartEvent) => {
    bringCardToFront(String(event.active.id))
  }, [bringCardToFront])

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const id = String(event.active.id)
      const card = cardMap.get(id)
      if (!card || columnWidth <= 0) return
      updateLayout((current) => {
        const currentPosition = current.positions[id]
        if (!currentPosition) return current
        const nextPosition = clampCardPosition(
          {
            ...currentPosition,
            x: currentPosition.x + event.delta.x / (columnWidth + CANVAS_GAP),
            y: currentPosition.y + event.delta.y / (CANVAS_ROW_HEIGHT + CANVAS_GAP),
          },
          card
        )
        return {
          ...current,
          positions: {
            ...current.positions,
            [id]: nextPosition,
          },
        }
      })
    },
    [cardMap, columnWidth, updateLayout]
  )

  const hideCard = useCallback((id: string) => {
    updateLayout((current) => ({ ...current, hidden: Array.from(new Set([...current.hidden, id])) }))
  }, [updateLayout])

  const restoreCard = useCallback((id: string) => {
    updateLayout((current) => ({ ...current, hidden: current.hidden.filter((item) => item !== id) }))
  }, [updateLayout])

  const resetLayout = useCallback(() => {
    updateLayout(() => ({
      order: allCardIds,
      hidden: [],
      positions: getDefaultCardPositions(allCards),
      zOrder: allCardIds,
    }))
  }, [allCardIds, allCards, updateLayout])

  const canvasHeight = useMemo(() => {
    if (!visibleCards.length) return CANVAS_MIN_HEIGHT
    const bottom = Math.max(
      ...visibleCards.map((card) => {
        const position = layout.positions[card.id] || { x: 0, y: 0, z: 1 }
        const size = cardGridSize(card.width)
        return position.y * (CANVAS_ROW_HEIGHT + CANVAS_GAP) + size.height * CANVAS_ROW_HEIGHT + (size.height - 1) * CANVAS_GAP
      })
    )
    return Math.max(CANVAS_MIN_HEIGHT, bottom)
  }, [layout.positions, visibleCards])

  const controls = (
    <div className="flex flex-wrap items-center justify-end gap-2">
      <Button variant="outline" size="sm" onClick={resetLayout} className="gap-2">
        <RotateCcw className="h-4 w-4" />
        {t('home.cards.reset')}
      </Button>
      <Button variant="outline" size="sm" onClick={() => setDialogOpen(true)} className="gap-2">
        <Plus className="h-4 w-4" />
        {t('home.cards.add')}
      </Button>
      <Button variant={editing ? 'default' : 'outline'} size="sm" onClick={() => setEditing((value) => !value)} className="gap-2">
        <GripVertical className="h-4 w-4" />
        {editing ? t('home.cards.done') : t('home.cards.edit')}
      </Button>
    </div>
  )

  return (
    <TooltipProvider>
      <div className="space-y-3">
        {controlsPortalId && controlsContainer ? createPortal(controls, controlsContainer) : null}
        {!controlsPortalId && controls}

        <DndContext sensors={sensors} onDragStart={handleDragStart} onDragEnd={handleDragEnd}>
          <div
            ref={canvasRef}
            data-home-summary-cards="true"
            className={cn(
              'relative min-h-[220px] w-full overflow-visible transition-[background] duration-150',
              editing && 'rounded-md bg-[linear-gradient(to_right,hsl(var(--border)/0.34)_1px,transparent_1px),linear-gradient(to_bottom,hsl(var(--border)/0.34)_1px,transparent_1px)]'
            )}
            style={{
              backgroundSize: editing ? `${columnWidth + CANVAS_GAP}px ${CANVAS_ROW_HEIGHT + CANVAS_GAP}px` : undefined,
              height: canvasHeight,
            }}
          >
            {visibleCards.map((card) => {
              const position = layout.positions[card.id] || { x: 0, y: 0, z: 1 }
              return (
                <FreeformHomeCard
                  key={card.id}
                  card={card}
                  columnWidth={columnWidth}
                  editing={editing}
                  onHide={hideCard}
                  position={position}
                  size={cardGridSize(card.width)}
                />
              )
            })}
          </div>
        </DndContext>

        <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>{t('home.cards.dialog.title')}</DialogTitle>
              <DialogDescription>{t('home.cards.dialog.description')}</DialogDescription>
            </DialogHeader>
            <DialogBody viewportClassName="max-h-[62vh]">
              <div className="space-y-5 pr-1">
                <div className="space-y-2">
                  <div className="text-sm font-medium">{t('home.cards.dialog.hiddenCards')}</div>
                  {hiddenCards.length === 0 ? (
                    <div className="rounded-md border border-dashed p-3 text-sm text-muted-foreground">
                      {t('home.cards.dialog.noHiddenCards')}
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {hiddenCards.map((card) => (
                        <div key={card.id} className="flex items-center justify-between gap-3 rounded-md border p-3">
                          <div className="min-w-0">
                            <div className="truncate text-sm font-medium">{card.title}</div>
                            {card.description && <div className="truncate text-xs text-muted-foreground">{card.description}</div>}
                          </div>
                          <Button variant="outline" size="sm" onClick={() => restoreCard(card.id)}>
                            {t('home.cards.dialog.restore')}
                          </Button>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

              </div>
            </DialogBody>
            <DialogFooter>
              <Button variant="outline" onClick={resetLayout} className="mr-auto gap-2">
                <RotateCcw className="h-4 w-4" />
                {t('home.cards.dialog.reset')}
              </Button>
              <Button variant="outline" onClick={() => setDialogOpen(false)}>
                {t('home.cards.dialog.cancel')}
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </TooltipProvider>
  )
}
