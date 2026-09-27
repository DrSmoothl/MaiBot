import { Settings } from 'lucide-react'
import { useId, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover'

export function ChatNicknameSettings({
  userName,
  onUpdateUserName,
}: {
  userName: string
  onUpdateUserName: (name: string) => void
}) {
  const { t } = useTranslation()
  const inputId = useId()
  const [open, setOpen] = useState(false)
  const [draftName, setDraftName] = useState(userName)

  const save = () => {
    onUpdateUserName(draftName.trim() || t('chat.userNameFallback'))
    setOpen(false)
  }

  return (
    <Popover
      open={open}
      onOpenChange={(nextOpen) => {
        if (nextOpen) setDraftName(userName)
        setOpen(nextOpen)
      }}
    >
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="h-9 w-9 shrink-0 rounded-full"
          aria-label={t('chat.sidebar.editName')}
          title={t('chat.sidebar.editName')}
        >
          <Settings className="h-4 w-4" />
        </Button>
      </PopoverTrigger>
      <PopoverContent side="top" align="start" className="w-64 space-y-3">
        <Label htmlFor={inputId}>{t('chat.identity.editName')}</Label>
        <Input
          id={inputId}
          value={draftName}
          placeholder={t('chat.identity.namePlaceholder')}
          onChange={(event) => setDraftName(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === 'Enter' && !event.nativeEvent.isComposing) {
              event.preventDefault()
              save()
            }
          }}
        />
        <div className="flex justify-end gap-2">
          <Button type="button" variant="ghost" size="sm" onClick={() => setOpen(false)}>
            {t('chat.actions.cancel')}
          </Button>
          <Button type="button" size="sm" onClick={save}>
            {t('chat.sidebar.saveName')}
          </Button>
        </div>
      </PopoverContent>
    </Popover>
  )
}
