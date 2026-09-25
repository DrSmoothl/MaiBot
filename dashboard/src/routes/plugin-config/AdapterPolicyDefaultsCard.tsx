import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { Button } from '@/components/ui/button'
import { useToast } from '@/hooks/use-toast'
import {
  CHAT_ADAPTER_STATUS_QUERY_KEY,
  getAdapterPolicyDefaults,
  updateAdapterPolicyDefaults,
  type AdapterPolicyDefaults,
} from '@/lib/chat-management-api'

export function AdapterPolicyDefaultsCard() {
  const queryClient = useQueryClient()
  const { toast } = useToast()
  const defaultsQuery = useQuery({
    queryKey: ['adapter-policy-defaults'],
    queryFn: getAdapterPolicyDefaults,
  })
  const defaultsMutation = useMutation({
    mutationFn: updateAdapterPolicyDefaults,
    onSuccess: (defaults) => {
      queryClient.setQueryData(['adapter-policy-defaults'], defaults)
      void queryClient.invalidateQueries({ queryKey: ['chat-stream-detail'] })
      void queryClient.invalidateQueries({ queryKey: [CHAT_ADAPTER_STATUS_QUERY_KEY] })
      void queryClient.invalidateQueries({ queryKey: ['adapter-host-policy'] })
      toast({ title: '适配器默认策略已保存' })
    },
    onError: (error) => {
      toast({
        title: '适配器默认策略保存失败',
        description: error instanceof Error ? error.message : '请稍后重试',
        variant: 'destructive',
      })
    },
  })

  const saveDefaultPolicy = (chatType: keyof AdapterPolicyDefaults, action: 'allow' | 'block') => {
    if (!defaultsQuery.data || defaultsQuery.data[chatType] === action) return
    defaultsMutation.mutate({ ...defaultsQuery.data, [chatType]: action })
  }

  return (
    <section className="space-y-3 rounded-md border p-4" aria-label="适配器全局默认策略">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="min-w-0">
          <h2 className="font-medium">麦麦默认策略</h2>
          <p className="text-muted-foreground text-xs">没设置额外规则时的策略</p>
        </div>
        {defaultsQuery.isError ? (
          <p className="text-destructive text-sm">默认策略加载失败</p>
        ) : (
          <div className="flex flex-wrap items-center gap-3 sm:gap-4">
            {(['group', 'private'] as const).map((chatType) => {
              const action = defaultsQuery.data?.[chatType]
              return (
                <div key={chatType} className="flex items-center gap-2">
                  <span className="text-sm">{chatType === 'group' ? '群聊' : '私聊'}</span>
                  <div className="flex gap-1">
                    {(['allow', 'block'] as const).map((option) => (
                      <Button
                        key={option}
                        type="button"
                        size="sm"
                        variant={
                          action === option
                            ? option === 'allow'
                              ? 'secondary'
                              : 'destructive'
                            : 'outline'
                        }
                        disabled={!action || defaultsMutation.isPending}
                        onClick={() => saveDefaultPolicy(chatType, option)}
                      >
                        {option === 'allow' ? '放行' : '拒绝'}
                      </Button>
                    ))}
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </div>
    </section>
  )
}
