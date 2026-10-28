import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Loader2 } from 'lucide-react'
import { useEffect, useState } from 'react'

import { Alert, AlertDescription } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card, CardContent } from '@/components/ui/card'
import { getConfigWidgets, invokeConfigWidget } from '@/lib/plugin-api/config'
import { cn } from '@/lib/utils'

/** 运行时数据独立于配置草稿；离开页面后不保留登录二维码。 */
export function ConfigWidgetsPanel({ pluginId }: { pluginId: string }) {
  const queryClient = useQueryClient()
  const queryKey = ['plugin-config-widgets', pluginId] as const
  const [now, setNow] = useState(Date.now)
  const action = useMutation({
    mutationFn: (actionId: string) => invokeConfigWidget(pluginId, actionId),
    retry: false,
    gcTime: 0,
    onMutate: async () => {
      await queryClient.cancelQueries({ queryKey })
    },
    onSuccess: (snapshot) => {
      if (queryClient.getQueryCache().find({ queryKey })?.getObserversCount()) {
        queryClient.setQueryData(queryKey, snapshot)
      }
    },
  })
  const snapshot = useQuery({
    queryKey,
    queryFn: ({ signal }) => getConfigWidgets(pluginId, signal),
    enabled: !action.isPending,
    refetchInterval: 2000,
    refetchIntervalInBackground: false,
    gcTime: 0,
    staleTime: 0,
    retry: false,
  })
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1000)
    return () => window.clearInterval(timer)
  }, [])
  const error = action.error ?? snapshot.error
  return (
    <Card className="mb-4">
      <CardContent className="space-y-3 pt-4">
        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error.message}</AlertDescription>
          </Alert>
        )}
        {snapshot.isPending && (
          <div role="status" className="flex items-center gap-2 text-sm">
            <Loader2 className="h-4 w-4 animate-spin" />
            正在获取运行状态
          </div>
        )}
        {!snapshot.isError &&
          snapshot.data?.widgets.map((widget) => {
            const state = snapshot.data.states[widget.id]
            if (!state.visible) return null
            const expired = state.expires_at != null && now >= state.expires_at * 1000
            if (widget.kind === 'action') {
              return (
                <Button
                  key={widget.id}
                  type="button"
                  variant="outline"
                  className="mr-2"
                  disabled={state.disabled || action.isPending || expired}
                  onClick={() => {
                    if (widget.confirmation && !window.confirm(widget.confirmation)) return
                    action.mutate(widget.id)
                  }}
                >
                  {action.isPending && action.variables === widget.id && (
                    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  )}
                  {widget.label}
                </Button>
              )
            }
            return (
              <div key={widget.id} className="space-y-2">
                <div className="text-sm font-medium">{widget.label}</div>
                {widget.kind === 'qrcode' &&
                  !expired &&
                  state.image &&
                  /^data:image\/(png|jpeg);base64,[A-Za-z0-9+/=]+$/.test(state.image) && (
                    <img
                      src={state.image}
                      alt={widget.label}
                      className="h-auto w-56 max-w-full bg-white p-2"
                    />
                  )}
                <p
                  role="status"
                  className={cn(
                    'text-sm break-words whitespace-pre-wrap',
                    state.tone === 'error'
                      ? 'text-destructive'
                      : state.tone === 'success'
                        ? 'text-green-600'
                        : state.tone === 'warning'
                          ? 'text-amber-600'
                          : 'text-muted-foreground'
                  )}
                >
                  {expired ? '二维码已过期，请刷新' : state.text}
                </p>
              </div>
            )
          })}
      </CardContent>
    </Card>
  )
}
