import { useTranslation } from 'react-i18next'

import { ComputerDesktop } from '@/components/agent/ComputerDesktop'
import { PageFrame } from '@/components/ui/surface'

export function ComputerPage() {
  const { t } = useTranslation()
  return <PageFrame className="flex h-full min-h-0 flex-col">
    <header className="border-b p-4"><h1 className="font-semibold">{t('chat.computerDesktop')}</h1></header>
    <ComputerDesktop />
  </PageFrame>
}
