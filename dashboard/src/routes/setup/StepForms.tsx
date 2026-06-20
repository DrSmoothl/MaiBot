import { Eye, EyeOff } from 'lucide-react'
import { useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import { Textarea } from '@/components/ui/textarea'

import type {
  ApiProviderSetupConfig,
  BotBasicConfig,
  ModelSetupConfig,
  PersonalityConfig,
} from './types'

interface BotBasicFormProps {
  config: BotBasicConfig
  onChange: (config: BotBasicConfig) => void
}

export function BotBasicForm({ config, onChange }: BotBasicFormProps) {
  const { t } = useTranslation()

  return (
    <div className="space-y-6">
      <div className="space-y-3">
        <Label htmlFor="nickname">{t('setupPage.forms.botBasic.nickname.label')}</Label>
        <Input
          id="nickname"
          placeholder={t('setupPage.forms.botBasic.nickname.placeholder')}
          value={config.nickname}
          onChange={(e) => onChange({ ...config, nickname: e.target.value })}
        />
        <p className="text-muted-foreground text-xs">
          {t('setupPage.forms.botBasic.nickname.description')}
        </p>
      </div>
    </div>
  )
}

// ====== 步骤2：人格配置 ======
interface PersonalityFormProps {
  config: PersonalityConfig
  onChange: (config: PersonalityConfig) => void
}

export function PersonalityForm({ config, onChange }: PersonalityFormProps) {
  const { t } = useTranslation()

  return (
    <div className="space-y-6">
      <div className="space-y-3">
        <Label htmlFor="personality">{t('setupPage.forms.personality.personality.label')}</Label>
        <Textarea
          id="personality"
          placeholder={t('setupPage.forms.personality.personality.placeholder')}
          value={config.personality}
          onChange={(e) => onChange({ ...config, personality: e.target.value })}
          rows={3}
        />
        <p className="text-muted-foreground text-xs">
          {t('setupPage.forms.personality.personality.description')}
        </p>
      </div>

      <div className="space-y-3">
        <Label htmlFor="reply_style">{t('setupPage.forms.personality.replyStyle.label')}</Label>
        <Textarea
          id="reply_style"
          placeholder={t('setupPage.forms.personality.replyStyle.placeholder')}
          value={config.reply_style}
          onChange={(e) => onChange({ ...config, reply_style: e.target.value })}
          rows={3}
        />
        <p className="text-muted-foreground text-xs">
          {t('setupPage.forms.personality.replyStyle.description')}
        </p>
      </div>
    </div>
  )
}

// ====== 步骤3：API 提供商配置 ======
interface ApiProviderSetupFormProps {
  config: ApiProviderSetupConfig
  onChange: (config: ApiProviderSetupConfig) => void
}

export function ApiProviderSetupForm({ config, onChange }: ApiProviderSetupFormProps) {
  const { t } = useTranslation()
  const [showApiKey, setShowApiKey] = useState(false)
  const apiKeyToggleLabel = showApiKey
    ? t('setupPage.forms.apiProvider.apiKey.hide')
    : t('setupPage.forms.apiProvider.apiKey.show')

  return (
    <div className="space-y-6">
      <div className="space-y-3">
        <Label htmlFor="provider_name">{t('setupPage.forms.apiProvider.providerName.label')}</Label>
        <Input
          id="provider_name"
          placeholder={t('setupPage.forms.apiProvider.providerName.placeholder')}
          value={config.provider_name}
          onChange={(e) => onChange({ ...config, provider_name: e.target.value })}
        />
        <p className="text-muted-foreground text-xs">
          {t('setupPage.forms.apiProvider.providerName.description')}
        </p>
      </div>

      <div className="space-y-3">
        <Label htmlFor="base_url">{t('setupPage.forms.apiProvider.baseUrl.label')}</Label>
        <Input
          id="base_url"
          placeholder="https://api.example.com/v1"
          value={config.base_url}
          onChange={(e) => onChange({ ...config, base_url: e.target.value })}
          className="font-mono"
        />
        <p className="text-muted-foreground text-xs">
          {t('setupPage.forms.apiProvider.baseUrl.description')}
        </p>
      </div>

      <div className="space-y-3">
        <Label htmlFor="api_key">{t('setupPage.forms.apiProvider.apiKey.label')}</Label>
        <div className="relative">
          <Input
            id="api_key"
            type={showApiKey ? 'text' : 'password'}
            placeholder="sk-..."
            value={config.api_key}
            onChange={(e) => onChange({ ...config, api_key: e.target.value })}
            className="pr-10 font-mono"
          />
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="absolute top-0 right-0 h-full px-3 hover:bg-transparent"
            onClick={() => setShowApiKey(!showApiKey)}
            aria-label={apiKeyToggleLabel}
            title={apiKeyToggleLabel}
          >
            {showApiKey ? (
              <EyeOff className="text-muted-foreground h-4 w-4" />
            ) : (
              <Eye className="text-muted-foreground h-4 w-4" />
            )}
          </Button>
        </div>
        <p className="text-muted-foreground text-xs">
          {t('setupPage.forms.apiProvider.apiKey.description')}
        </p>
      </div>
    </div>
  )
}

// ====== 步骤4：基础模型配置 ======
interface ModelSetupFormProps {
  config: ModelSetupConfig
  onChange: (config: ModelSetupConfig) => void
}

export function ModelSetupForm({ config, onChange }: ModelSetupFormProps) {
  const { t } = useTranslation()

  const inferThinkingEnabled = (modelIdentifier: string) => {
    const normalizedIdentifier = modelIdentifier.trim().toLowerCase()
    return normalizedIdentifier.includes('deepseek-v4-pro')
  }

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-2">
        <div className="space-y-4 rounded-lg border p-4">
          <div className="space-y-3">
            <Label htmlFor="planner_model_identifier">
              {t('setupPage.forms.modelSetup.planner.identifier.label')}
            </Label>
            <Input
              id="planner_model_identifier"
              placeholder="gpt-4.1-mini"
              value={config.planner_model_identifier}
              onChange={(e) =>
                onChange({
                  ...config,
                  planner_model_identifier: e.target.value,
                  planner_model_name: e.target.value,
                  planner_thinking: inferThinkingEnabled(e.target.value),
                })
              }
              className="font-mono"
            />
            <p className="text-muted-foreground text-xs">
              {t('setupPage.forms.modelSetup.planner.identifier.description')}
            </p>
          </div>

          <div className="flex items-center justify-between gap-4 rounded-md bg-muted/40 p-3">
            <Label htmlFor="planner_visual" className="text-sm font-medium">
              {t('setupPage.forms.modelSetup.planner.visual.label')}
            </Label>
            <Switch
              id="planner_visual"
              checked={config.planner_visual}
              onCheckedChange={(checked) =>
                onChange({ ...config, planner_visual: checked })
              }
            />
          </div>

          <div className="flex items-center justify-between gap-4 rounded-md bg-muted/40 p-3">
            <Label htmlFor="planner_thinking" className="text-sm font-medium">
              启用思考
            </Label>
            <Switch
              id="planner_thinking"
              checked={config.planner_thinking}
              onCheckedChange={(checked) =>
                onChange({ ...config, planner_thinking: checked })
              }
            />
          </div>
        </div>

        <div className="space-y-4 rounded-lg border p-4">
          <div className="space-y-3">
            <Label htmlFor="replyer_model_identifier">
              {t('setupPage.forms.modelSetup.replyer.identifier.label')}
            </Label>
            <Input
              id="replyer_model_identifier"
              placeholder="gpt-4.1"
              value={config.replyer_model_identifier}
              onChange={(e) =>
                onChange({
                  ...config,
                  replyer_model_identifier: e.target.value,
                  replyer_model_name: e.target.value,
                  replyer_thinking: inferThinkingEnabled(e.target.value),
                })
              }
              className="font-mono"
            />
            <p className="text-muted-foreground text-xs">
              {t('setupPage.forms.modelSetup.replyer.identifier.description')}
            </p>
          </div>

          <div className="flex items-center justify-between gap-4 rounded-md bg-muted/40 p-3">
            <Label htmlFor="replyer_visual" className="text-sm font-medium">
              {t('setupPage.forms.modelSetup.replyer.visual.label')}
            </Label>
            <Switch
              id="replyer_visual"
              checked={config.replyer_visual}
              onCheckedChange={(checked) =>
                onChange({ ...config, replyer_visual: checked })
              }
            />
          </div>

          <div className="flex items-center justify-between gap-4 rounded-md bg-muted/40 p-3">
            <Label htmlFor="replyer_thinking" className="text-sm font-medium">
              启用思考
            </Label>
            <Switch
              id="replyer_thinking"
              checked={config.replyer_thinking}
              onCheckedChange={(checked) =>
                onChange({ ...config, replyer_thinking: checked })
              }
            />
          </div>
        </div>
      </div>

      <div className="bg-muted/50 rounded-lg p-4 text-sm text-muted-foreground">
        {t('setupPage.forms.modelSetup.saveHint')}
      </div>
    </div>
  )
}
