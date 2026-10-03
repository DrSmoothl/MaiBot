import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertCircle, Loader2, Save, UsersRound } from 'lucide-react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'

import { ListFieldEditor } from '@/components/ListFieldEditor'
import { Alert, AlertDescription } from '@/components/ui/alert'
import { Avatar, AvatarFallback, AvatarImage } from '@/components/ui/avatar'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Label } from '@/components/ui/label'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import { useToast } from '@/hooks/use-toast'
import { useResolvedAvatarUrl } from '@/lib/avatar-url'
import { getAdapterHostPolicy, getAllChatStreams, updateAdapterHostPolicy } from '@/lib/chat-management-api'
import type {
  AdapterAccountEntry,
  AdapterActiveIdentity,
  AdapterHostDefaultAction,
  AdapterHostPolicy,
  AdapterPolicyDefaults,
  ChatStreamType,
} from '@/lib/chat-management-api'

interface AdapterHostPolicyPanelProps {
  pluginId: string
  /** 账号与保存工具栏的挂载点（页签同一行）；未提供时渲染在面板顶部 */
  toolbarContainer?: HTMLElement | null
}

interface AdapterHostPolicyEditorProps {
  policy: AdapterHostPolicy
  globalDefaults: AdapterPolicyDefaults
  activeIdentity: AdapterActiveIdentity | null | undefined
  hasEntry: boolean | undefined
  accountEntries: AdapterAccountEntry[] | undefined
  saveStatus: string | null
  manualSaveDisabled: boolean
  onManualSave: () => void
  toolbarContainer?: HTMLElement | null
  onSectionChange: (
    chatType: ChatStreamType,
    field: 'default_action' | 'allow_ids' | 'deny_ids',
    value: AdapterHostDefaultAction | string[]
  ) => void
}

/** 编辑停止后自动保存的防抖时长，与主程序配置页保持一致。 */
export const AUTOSAVE_DELAY_MS = 2000

function clonePolicy(policy: AdapterHostPolicy): AdapterHostPolicy {
  return {
    group: {
      default_action: policy.group.default_action,
      allow_ids: [...policy.group.allow_ids],
      deny_ids: [...policy.group.deny_ids],
    },
    private: {
      default_action: policy.private.default_action,
      allow_ids: [...policy.private.allow_ids],
      deny_ids: [...policy.private.deny_ids],
    },
  }
}

function formatSaveTime(timestamp: number): string {
  return new Date(timestamp).toLocaleTimeString('zh-CN', { hour12: false })
}

function GroupIdentity({
  groupId,
  name,
  identity,
}: {
  groupId: string
  name?: string
  identity: AdapterActiveIdentity
}) {
  const avatarUrl = useResolvedAvatarUrl(identity.platform, groupId, 'group', undefined, {
    accountId: identity.account_id,
    scope: identity.scope,
  })
  return (
    <div className="flex min-w-0 flex-1 items-center gap-2">
      <Avatar className="h-7 w-7">
        <AvatarImage src={avatarUrl} alt={name || groupId} />
        <AvatarFallback><UsersRound className="h-4 w-4 text-muted-foreground" /></AvatarFallback>
      </Avatar>
      {name && <span className="truncate text-sm" title={name}>{name}</span>}
    </div>
  )
}

function AdapterHostPolicyEditor({
  policy,
  globalDefaults,
  activeIdentity,
  hasEntry,
  accountEntries,
  saveStatus,
  manualSaveDisabled,
  onManualSave,
  toolbarContainer,
  onSectionChange,
}: AdapterHostPolicyEditorProps) {
  const streamsQuery = useQuery({
    queryKey: ['chat-streams', 'all'],
    queryFn: getAllChatStreams,
    enabled: Boolean(activeIdentity?.platform && (policy.group.allow_ids.length || policy.group.deny_ids.length)),
    staleTime: 60_000,
  })
  const groupNames = useMemo(() => {
    const names = new Map<string, string>()
    if (!activeIdentity) return names
    for (const stream of streamsQuery.data ?? []) {
      if (stream.platform !== activeIdentity.platform || !stream.group_id || !stream.group_name?.trim()) continue
      // 同一群有多个聊天流时，优先使用当前账号和连接记录的真实群名称。
      if (!names.has(stream.group_id) || (
        stream.account_id === activeIdentity.account_id && stream.scope === activeIdentity.scope
      )) names.set(stream.group_id, stream.group_name.trim())
    }
    return names
  }, [streamsQuery.data, activeIdentity])
  const renderGroupIdentity = (value: unknown) => {
    if (!activeIdentity || typeof value !== 'string') return null
    const trimmedId = value.trim()
    const platformPrefix = `${activeIdentity.platform}:`
    const groupId = trimmedId.startsWith(platformPrefix) ? trimmedId.slice(platformPrefix.length) : trimmedId
    if (!groupId || groupId.includes('*') || groupId.includes(':')) return null
    return <GroupIdentity groupId={groupId} name={groupNames.get(groupId)} identity={activeIdentity} />
  }
  // 当前账号之外的条目：换账号登录后旧条目保留在配置中但不再生效
  // 同一账号可能在多个网关下各有条目，按账号 ID 去重
  const staleAccounts = [
    ...new Set(
      (accountEntries ?? [])
        .map((entry) => entry.account_id)
        .filter((id) => id && id !== (activeIdentity?.account_id ?? ''))
    ),
  ]

  const toolbar = (
    <div className="flex flex-wrap items-center justify-between gap-3">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="secondary" data-testid="active-account-badge">
          当前账号 ID：{activeIdentity?.account_id || '未获取'}
        </Badge>
        {hasEntry === false && (
          <Badge variant="outline">无专属规则，按全局默认生效</Badge>
        )}
      </div>
      <div className="flex flex-wrap items-center gap-3">
        {saveStatus && (
          <span className="text-muted-foreground text-xs" data-testid="host-policy-save-status">
            {saveStatus}
          </span>
        )}
        <Button
          type="button"
          size="sm"
          variant="outline"
          aria-label="保存"
          title="保存"
          disabled={manualSaveDisabled}
          onClick={onManualSave}
        >
          <Save className="h-4 w-4" />
        </Button>
      </div>
    </div>
  )

  return (
    <div className="space-y-4">
      {toolbarContainer ? createPortal(toolbar, toolbarContainer) : toolbar}
      {hasEntry === false && staleAccounts.length > 0 && (
        <Alert>
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>
            规则按账号 ID 区分：当前账号（{activeIdentity?.account_id || '未知'}）还没有专属规则，以下编辑保存后将为其新建；历史账号
            {staleAccounts.map((id) => ` ${id}`).join('、')} 的规则保留在配置中，但对当前账号不再生效。
          </AlertDescription>
        </Alert>
      )}
      <div className="grid gap-4 grid-cols-1 sm:grid-cols-2">
        {(['group', 'private'] as const).map((chatType) => {
          const section = policy[chatType]
          const globalAction = globalDefaults[chatType]
          const title = chatType === 'group' ? '群聊规则' : '私聊规则'
          const chatLabel = chatType === 'group' ? '群聊' : '私聊'
          const idLabel = chatType === 'group' ? '群号' : '用户 ID'
          const effectiveAction =
            section.default_action === 'inherit' ? globalAction : section.default_action
          // 接收所有消息时「接收」名单不起作用；默认不接收时「不接收」名单只在接收名单含 * 时才有意义
          const allowInactive = effectiveAction === 'allow'
          const denyInactive = effectiveAction === 'block' && !section.allow_ids.includes('*')
          const modeHint =
            effectiveAction === 'allow'
              ? `黑名单模式：接收所有${chatLabel}消息，只需在「不接收消息的聊天ID」中添加要屏蔽的${idLabel}。`
              : `白名单模式：默认不接收${chatLabel}消息，只需在「接收消息的聊天ID」中添加要接收的${idLabel}。`
          return (
            <Card key={chatType}>
              <CardHeader>
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <CardTitle className="text-base">{title}</CardTitle>
                    <CardDescription>不接收消息的聊天 ID 优先于接收消息的聊天 ID，支持使用 * 匹配全部。</CardDescription>
                  </div>
                </div>
              </CardHeader>
              <CardContent className="space-y-5">
                <div className="space-y-2">
                  <Label>默认规则：</Label>
                  <Select
                    value={section.default_action}
                    onValueChange={(value) =>
                      onSectionChange(chatType, 'default_action', value as AdapterHostDefaultAction)
                    }
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="allow">接收所有消息</SelectItem>
                      <SelectItem value="block">默认不接收消息</SelectItem>
                      <SelectItem value="inherit">
                        与全局设置一致（{globalAction === 'allow' ? '接收所有消息' : '默认不接收消息'}）
                      </SelectItem>
                    </SelectContent>
                  </Select>
                  <p className="text-muted-foreground text-xs" data-testid={`mode-hint:${chatType}`}>
                    {modeHint}
                  </p>
                </div>

                <div
                  className={allowInactive ? 'space-y-2 opacity-50' : 'space-y-2'}
                  data-testid={`allow-section:${chatType}`}
                  data-inactive={allowInactive}
                >
                  <div>
                    <Label>接收消息的聊天ID</Label>
                    {allowInactive && (
                      <p className="text-muted-foreground mt-1 text-xs">
                        当前接收所有消息，此列表不生效。
                      </p>
                    )}
                  </div>
                  <ListFieldEditor
                    // 空列表时直接禁用；残留条目仍可编辑，方便清理
                    disabled={allowInactive && section.allow_ids.length === 0}
                    value={section.allow_ids}
                    onChange={(value) =>
                      onSectionChange(
                        chatType,
                        'allow_ids',
                        value.map((item) => String(item))
                      )
                    }
                    itemType="string"
                    emptyText="空列表"
                    placeholder={chatType === 'group' ? '输入接收消息的群号' : '输入接收消息的用户 ID'}
                    renderItemSuffix={chatType === 'group' ? renderGroupIdentity : undefined}
                  />
                </div>

                <div
                  className={denyInactive ? 'space-y-2 opacity-50' : 'space-y-2'}
                  data-testid={`deny-section:${chatType}`}
                  data-inactive={denyInactive}
                >
                  <div>
                    <Label>不接收消息的聊天ID</Label>
                    {denyInactive && (
                      <p className="text-muted-foreground mt-1 text-xs">
                        当前默认不接收消息，此列表不生效。
                      </p>
                    )}
                  </div>
                  <ListFieldEditor
                    disabled={denyInactive && section.deny_ids.length === 0}
                    value={section.deny_ids}
                    onChange={(value) =>
                      onSectionChange(
                        chatType,
                        'deny_ids',
                        value.map((item) => String(item))
                      )
                    }
                    itemType="string"
                    emptyText="空列表"
                    placeholder={chatType === 'group' ? '输入不接收消息的群号' : '输入不接收消息的用户 ID'}
                    renderItemSuffix={chatType === 'group' ? renderGroupIdentity : undefined}
                  />
                </div>
              </CardContent>
            </Card>
          )
        })}
      </div>
    </div>
  )
}

export function AdapterHostPolicyPanel({
  pluginId,
  toolbarContainer,
}: AdapterHostPolicyPanelProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const queryKey = ['adapter-host-policy', pluginId] as const
  const policyQuery = useQuery({
    queryKey,
    queryFn: () => getAdapterHostPolicy(pluginId),
    // 适配器换账号登录后当前激活身份会变化，定期拉取保证面板展示的规则归属不脱靶
    refetchInterval: 30_000,
  })
  const serverPolicy = policyQuery.data?.policy
  const serverPolicyText = serverPolicy ? JSON.stringify(serverPolicy) : null
  // 草稿与保存状态放在面板层：回包只更新内容，不重挂载组件；
  // 与服务端数据比较一律用内容快照，结构化共享会保留相同内容的旧对象引用。
  const [policy, setPolicy] = useState<AdapterHostPolicy | null>(() =>
    serverPolicy ? clonePolicy(serverPolicy) : null
  )
  // 保存成功时间按内容记录：草稿内容变化后自动失效，切换插件不会残留过期状态
  const [lastSaved, setLastSaved] = useState<{ text: string; at: number } | null>(null)
  const serverSnapshotRef = useRef<string | null>(serverPolicyText)
  const pendingSaveRef = useRef<AdapterHostPolicy | null>(null)
  const autosaveTimerRef = useRef<number | null>(null)

  const hasUnsavedChanges =
    policy !== null && serverPolicyText !== null && JSON.stringify(policy) !== serverPolicyText

  const saveMutation = useMutation({
    mutationFn: (next: AdapterHostPolicy) => updateAdapterHostPolicy(pluginId, next),
    onSuccess: (response) => {
      serverSnapshotRef.current = JSON.stringify(response.policy)
      setPolicy(clonePolicy(response.policy))
      setLastSaved({ text: JSON.stringify(response.policy), at: Date.now() })
      queryClient.setQueryData(queryKey, response)
      void queryClient.invalidateQueries({ queryKey: ['chat-stream-detail'] })
    },
    onError: (error) => {
      toast({
        title: '主程序放行规则保存失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    },
  })
  const { mutate } = saveMutation

  // 服务端内容变化（切换插件或重新拉取）时重置草稿，实现热重载
  useEffect(() => {
    if (serverPolicyText === null || serverPolicy === undefined) {
      return
    }
    if (serverPolicyText === serverSnapshotRef.current) {
      return
    }
    serverSnapshotRef.current = serverPolicyText
    setPolicy(clonePolicy(serverPolicy))
  }, [serverPolicy, serverPolicyText])

  // 自动保存：编辑停止后写回后端
  useEffect(() => {
    if (!hasUnsavedChanges || !policy) {
      return
    }
    pendingSaveRef.current = policy
    const timer = window.setTimeout(() => {
      autosaveTimerRef.current = null
      pendingSaveRef.current = null
      mutate(policy)
    }, AUTOSAVE_DELAY_MS)
    autosaveTimerRef.current = timer
    return () => {
      window.clearTimeout(timer)
      if (autosaveTimerRef.current === timer) {
        autosaveTimerRef.current = null
      }
    }
  }, [policy, hasUnsavedChanges, mutate])

  // 切换页签会卸载面板；立即提交尚在防抖期的最新草稿，避免编辑丢失
  useEffect(
    () => () => {
      if (pendingSaveRef.current) {
        mutate(pendingSaveRef.current)
        pendingSaveRef.current = null
      }
    },
    [mutate]
  )

  // 手动保存：取消尚未执行的自动保存，立即写回当前草稿
  const saveNow = () => {
    if (autosaveTimerRef.current !== null) {
      window.clearTimeout(autosaveTimerRef.current)
      autosaveTimerRef.current = null
    }
    pendingSaveRef.current = null
    if (policy) {
      mutate(policy)
    }
  }

  if (policyQuery.isLoading) {
    return (
      <div className="text-muted-foreground flex h-48 items-center justify-center gap-2 text-sm">
        <Loader2 className="h-4 w-4 animate-spin" />
        正在加载主程序规则
      </div>
    )
  }

  if (policyQuery.isError || !policyQuery.data || !policy) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>
          {policyQuery.error instanceof Error ? policyQuery.error.message : '主程序规则加载失败'}
        </AlertDescription>
      </Alert>
    )
  }

  const saveStatus = saveMutation.isPending
    ? '自动保存中'
    : saveMutation.isError
      ? '自动保存失败'
      : hasUnsavedChanges
        ? '未保存的更改'
        : lastSaved && lastSaved.text === JSON.stringify(policy)
          ? `已保存 ${formatSaveTime(lastSaved.at)}`
          : null

  return (
    <AdapterHostPolicyEditor
      policy={policy}
      globalDefaults={policyQuery.data.global_defaults}
      activeIdentity={policyQuery.data.active_identity}
      hasEntry={policyQuery.data.has_entry}
      accountEntries={policyQuery.data.account_entries}
      saveStatus={saveStatus}
      manualSaveDisabled={!hasUnsavedChanges || saveMutation.isPending}
      onManualSave={saveNow}
      toolbarContainer={toolbarContainer}
      onSectionChange={(chatType, field, value) =>
        setPolicy((current) =>
          current
            ? {
                ...current,
                [chatType]: {
                  ...current[chatType],
                  [field]: value,
                },
              }
            : current
        )
      }
    />
  )
}
