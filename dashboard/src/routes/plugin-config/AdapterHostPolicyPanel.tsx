import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { AlertCircle, Loader2, Save } from 'lucide-react'
import { useEffect, useRef, useState } from 'react'

import { ListFieldEditor } from '@/components/ListFieldEditor'
import { Alert, AlertDescription } from '@/components/ui/alert'
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
import { getAdapterHostPolicy, updateAdapterHostPolicy } from '@/lib/chat-management-api'
import type {
  AdapterHostDefaultAction,
  AdapterHostPolicy,
  AdapterPolicyDefaults,
  ChatStreamType,
} from '@/lib/chat-management-api'

interface AdapterHostPolicyPanelProps {
  pluginId: string
}

interface AdapterHostPolicyEditorProps {
  policy: AdapterHostPolicy
  globalDefaults: AdapterPolicyDefaults
  saveStatus: string | null
  manualSaveDisabled: boolean
  onManualSave: () => void
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

function AdapterHostPolicyEditor({
  policy,
  globalDefaults,
  saveStatus,
  manualSaveDisabled,
  onManualSave,
  onSectionChange,
}: AdapterHostPolicyEditorProps) {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <p className="text-muted-foreground text-xs">编辑后 2 秒自动保存</p>
        <div className="flex items-center gap-2">
          {saveStatus && (
            <span className="text-muted-foreground text-xs" data-testid="host-policy-save-status">
              {saveStatus}
            </span>
          )}
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={manualSaveDisabled}
            onClick={onManualSave}
          >
            <Save className="h-4 w-4" />
            保存
          </Button>
        </div>
      </div>
      <div className="grid gap-4 grid-cols-1 sm:grid-cols-2">
        {(['group', 'private'] as const).map((chatType) => {
          const section = policy[chatType]
          const globalAction = globalDefaults[chatType]
          const title = chatType === 'group' ? '群聊规则' : '私聊规则'
          return (
            <Card key={chatType}>
              <CardHeader>
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <CardTitle className="text-base">{title}</CardTitle>
                    <CardDescription>不阅读的聊天 ID 优先于阅读的聊天 ID，支持使用 * 匹配全部。</CardDescription>
                  </div>
                  <Badge variant="outline">
                    全局默认：{globalAction === 'allow' ? '阅读' : '不阅读'}
                  </Badge>
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
                      <SelectItem value="inherit">
                        默认
                      </SelectItem>
                      <SelectItem value="block">不阅读</SelectItem>
                      <SelectItem value="allow">阅读</SelectItem>
                    </SelectContent>
                  </Select>
                </div>

                <div className="space-y-2">
                  <div>
                    <Label>阅读的聊天ID</Label>
                    <p className="text-muted-foreground mt-1 text-xs">
                      群聊填写群号，私聊填写用户 ID。
                    </p>
                  </div>
                  <ListFieldEditor
                    value={section.allow_ids}
                    onChange={(value) =>
                      onSectionChange(
                        chatType,
                        'allow_ids',
                        value.map((item) => String(item))
                      )
                    }
                    itemType="string"
                    placeholder={chatType === 'group' ? '输入需要阅读的群号' : '输入需要阅读的用户 ID'}
                  />
                </div>

                <div className="space-y-2">
                  <div>
                    <Label>不阅读的聊天ID</Label>
                    <p className="text-muted-foreground mt-1 text-xs">
                      同一 ID 不可同时出现在阅读和不阅读列表。
                    </p>
                  </div>
                  <ListFieldEditor
                    value={section.deny_ids}
                    onChange={(value) =>
                      onSectionChange(
                        chatType,
                        'deny_ids',
                        value.map((item) => String(item))
                      )
                    }
                    itemType="string"
                    placeholder={chatType === 'group' ? '输入不阅读的群号' : '输入不阅读的用户 ID'}
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

export function AdapterHostPolicyPanel({ pluginId }: AdapterHostPolicyPanelProps) {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const queryKey = ['adapter-host-policy', pluginId] as const
  const policyQuery = useQuery({
    queryKey,
    queryFn: () => getAdapterHostPolicy(pluginId),
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
      saveStatus={saveStatus}
      manualSaveDisabled={!hasUnsavedChanges || saveMutation.isPending}
      onManualSave={saveNow}
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
