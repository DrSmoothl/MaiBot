import { useId, useState } from 'react'

import { ChevronDown, ChevronUp } from 'lucide-react'

import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { Label } from '@/components/ui/label'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Separator } from '@/components/ui/separator'
import { Switch } from '@/components/ui/switch'
import type { FieldHookComponent } from '@/lib/field-hooks'

import { AMemorixRetrievalChatsHook } from './AMemorixRetrievalChatsHook'
import {
  resolveAMemorixRetrievalChatsCopy,
  resolveAMemorixRetrievalFilterMirrorKind,
  type RetrievalFilterKind,
} from './AMemorixRetrievalChatsHook.utils'

type RetrievalFilterMode = 'blacklist' | 'whitelist'

interface RetrievalSubtypeFilterValue {
  chats: string[]
  enabled: boolean
  mode: RetrievalFilterMode
}

const getObjectValue = (value: unknown): Record<string, unknown> => {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {}
}

const getNestedValue = (source: Record<string, unknown>, path: string[]): unknown => {
  let current: unknown = source
  for (const key of path) {
    if (!current || typeof current !== 'object' || Array.isArray(current)) {
      return undefined
    }
    current = (current as Record<string, unknown>)[key]
  }
  return current
}

const normalizeEnabledValue = (value: unknown): boolean => {
  if (typeof value === 'boolean') {
    return value
  }
  return String(value ?? '').trim().toLowerCase() === 'true'
}

const normalizeSubtypeFilterValue = (
  parentValues: Record<string, unknown> | undefined,
  kind: RetrievalFilterKind,
): RetrievalSubtypeFilterValue => {
  const subtypeConfig = getObjectValue(
    getNestedValue(parentValues ?? {}, ['filter', 'retrieval', kind]),
  )
  const rawMode = String(subtypeConfig.mode ?? '').trim()
  return {
    chats: Array.isArray(subtypeConfig.chats)
      ? subtypeConfig.chats.map((item) => String(item ?? '').trim()).filter(Boolean)
      : [],
    enabled: normalizeEnabledValue(subtypeConfig.enabled),
    mode: rawMode === 'whitelist' ? 'whitelist' : 'blacklist',
  }
}

const configPathFor = (kind: RetrievalFilterKind, field: keyof RetrievalSubtypeFilterValue) => {
  return `filter.retrieval.${kind}.${field}`
}

export const AMemorixRetrievalFilterMirrorHook: FieldHookComponent = ({
  children,
  fieldPath,
  onParentChange,
  parentValues,
}) => {
  const kind = resolveAMemorixRetrievalFilterMirrorKind(fieldPath)
  const filterValue = normalizeSubtypeFilterValue(parentValues, kind)
  const chatsFieldPath = `a_memorix.filter.retrieval.${kind}.chats`
  const copy = resolveAMemorixRetrievalChatsCopy(chatsFieldPath)
  const [open, setOpen] = useState(false)
  const contentId = useId()
  const modeText = filterValue.mode === 'whitelist' ? '白名单' : '黑名单'
  const enabledText = filterValue.enabled ? '已启用' : '未启用'
  const selectedCount = filterValue.chats.length

  return (
    <div className="min-w-0 space-y-5">
      {children}

      <Separator className="bg-border/60" />

      <Collapsible open={open} onOpenChange={setOpen} className="min-w-0 rounded-md border bg-muted/10">
        <div className="flex min-w-0 items-start justify-between gap-3 px-3 py-3">
          <div className="min-w-0 space-y-1">
            <div className="flex min-w-0 flex-wrap items-center gap-2">
              <Label className="text-[15px] font-medium leading-6 text-foreground">
                检索结果过滤范围
              </Label>
              <Badge variant="outline" className="shrink-0">
                {copy.badge}
              </Badge>
            </div>
            <p className="text-xs leading-5 text-muted-foreground">
              {enabledText}，{modeText}，已选择 {selectedCount} 个聊天流 token。
            </p>
          </div>
          <CollapsibleTrigger asChild>
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="h-8 w-8 shrink-0 px-0"
              aria-controls={contentId}
              aria-expanded={open}
              aria-label={open ? '收起检索结果过滤范围' : '展开检索结果过滤范围'}
              title={open ? '收起' : '展开'}
            >
              {open ? (
                <ChevronUp className="h-4 w-4" aria-hidden="true" />
              ) : (
                <ChevronDown className="h-4 w-4" aria-hidden="true" />
              )}
            </Button>
          </CollapsibleTrigger>
        </div>

        <CollapsibleContent id={contentId} className="border-t px-3 py-4">
          <div className="min-w-0 space-y-4">
            <div className="grid min-w-0 gap-4 md:grid-cols-2">
              <div className="flex min-w-0 items-center justify-between gap-4 rounded-md border bg-background/60 px-3 py-2">
                <div className="min-w-0">
                  <Label className="text-sm font-medium">启用{copy.badge}过滤</Label>
                  <p className="mt-1 text-xs leading-5 text-muted-foreground">
                    关闭时保留原有检索行为。
                  </p>
                </div>
                <Switch
                  checked={filterValue.enabled}
                  onCheckedChange={(checked) => {
                    onParentChange?.(configPathFor(kind, 'enabled'), checked)
                  }}
                />
              </div>

              <div className="min-w-0 space-y-2 rounded-md border bg-background/60 px-3 py-2">
                <Label className="text-sm font-medium">过滤模式</Label>
                <Select
                  value={filterValue.mode}
                  onValueChange={(mode) => {
                    onParentChange?.(
                      configPathFor(kind, 'mode'),
                      mode === 'whitelist' ? 'whitelist' : 'blacklist',
                    )
                  }}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="blacklist">黑名单</SelectItem>
                    <SelectItem value="whitelist">白名单</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>

            <AMemorixRetrievalChatsHook
              fieldPath={chatsFieldPath}
              onChange={(nextChats) => {
                onParentChange?.(configPathFor(kind, 'chats'), nextChats)
              }}
              schema={{
                name: 'chats',
                type: 'array',
                label: '聊天流列表',
                description: '聊天流列表',
                required: false,
              }}
              value={filterValue.chats}
            />
          </div>
        </CollapsibleContent>
      </Collapsible>
    </div>
  )
}
