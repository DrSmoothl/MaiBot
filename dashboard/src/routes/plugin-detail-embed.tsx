import { EmbedPageShell } from '@/components/embed-page-shell'

import { PluginDetailPage } from './plugin-detail'

export function PluginDetailEmbedPage() {
  return (
    <EmbedPageShell shellId="embed-plugin-detail" title="插件详情 - MaiBot Dashboard">
      <PluginDetailPage embedded />
    </EmbedPageShell>
  )
}
