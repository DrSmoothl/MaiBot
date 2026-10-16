import { CheckCircle2, Eye, EyeOff, KeyRound, ShieldCheck, XCircle } from 'lucide-react'
import { AnimatePresence, motion, useReducedMotion } from 'motion/react'
import { useEffect, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { HelpTooltip } from '@/components/ui/help-tooltip'
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from '@/components/ui/select'
import { Switch } from '@/components/ui/switch'
import { Textarea } from '@/components/ui/textarea'
import { validateToken } from '@/lib/token-validator'
import { cn } from '@/lib/utils'
import type { ModelListItem } from '@/lib/config-api'
import { findTemplateByBaseUrl, PROVIDER_TEMPLATES, resolveThinkingFormatForModel } from '../config/providerTemplates'

import type {
  ApiProviderSetupConfig,
  BotBasicConfig,
  ModelSetupConfig,
  PersonalityConfig,
} from './types'

interface CustomTokenFormProps {
  token: string
  onChange: (token: string) => void
}

export function CustomTokenForm({ token, onChange }: CustomTokenFormProps) {
  const { t } = useTranslation()
  const [showToken, setShowToken] = useState(false)
  const tokenValidation = useMemo(() => validateToken(token), [token])
  const toggleLabel = showToken
    ? t('setupPage.forms.customToken.hide')
    : t('setupPage.forms.customToken.show')

  return (
    <div className="space-y-6">
      <div className="rounded-lg border border-yellow-200 bg-yellow-50 p-4 text-sm text-yellow-900 dark:border-yellow-900 dark:bg-yellow-950/30 dark:text-yellow-200">
        <div className="flex gap-3">
          <ShieldCheck className="mt-0.5 h-5 w-5 flex-shrink-0" strokeWidth={2} fill="none" />
          <div className="space-y-1">
            <p className="font-semibold">{t('setupPage.forms.customToken.noticeTitle')}</p>
            <p>{t('setupPage.forms.customToken.noticeDescription')}</p>
          </div>
        </div>
      </div>

      <div className="space-y-3">
        <Label htmlFor="custom-token">{t('setupPage.forms.customToken.label')}</Label>
        <div className="relative">
          <KeyRound
            className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground"
            strokeWidth={2}
            fill="none"
          />
          <Input
            id="custom-token"
            type={showToken ? 'text' : 'password'}
            placeholder={t('setupPage.forms.customToken.placeholder')}
            value={token}
            onChange={(e) => onChange(e.target.value)}
            className="pl-10 pr-10 font-mono"
            autoComplete="new-password"
          />
          <Button
            type="button"
            variant="ghost"
            size="sm"
            className="absolute top-1/2 right-2 h-7 w-7 -translate-y-1/2 rounded-md border-0 p-0 hover:bg-transparent"
            onClick={() => setShowToken(!showToken)}
            aria-label={toggleLabel}
            title={toggleLabel}
          >
            {showToken ? (
              <EyeOff className="text-muted-foreground h-4 w-4" />
            ) : (
              <Eye className="text-muted-foreground h-4 w-4" />
            )}
          </Button>
        </div>
        {t('setupPage.forms.customToken.description') ? (
          <p className="text-muted-foreground text-xs">
            {t('setupPage.forms.customToken.description')}
          </p>
        ) : null}
      </div>

      <div className="rounded-lg bg-muted/50 p-4">
        <div className="grid grid-cols-2 gap-x-6 gap-y-1.5">
          {tokenValidation.rules.map((rule) => (
            <div key={rule.id} className="flex items-center gap-2 text-xs">
              {rule.passed ? (
                <CheckCircle2 className="h-3.5 w-3.5 flex-shrink-0 text-green-500" />
              ) : (
                <XCircle className="h-3.5 w-3.5 flex-shrink-0 text-muted-foreground" />
              )}
              <span
                className={cn(
                  rule.passed ? 'text-green-600 dark:text-green-400' : 'text-muted-foreground'
                )}
              >
                {rule.label}
              </span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

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
    <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
      <div className="space-y-3">
        <Label htmlFor="personality">{t('setupPage.forms.personality.personality.label')}</Label>
        <Textarea
          id="personality"
          autoResize={false}
          className="h-32 resize-none overflow-y-auto"
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
          autoResize={false}
          className="h-32 resize-none overflow-y-auto"
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
  const reduceMotion = useReducedMotion()
  const [showApiKey, setShowApiKey] = useState(false)
  const [customMode, setCustomMode] = useState(false)
  const [selectedPreset, setSelectedPreset] = useState('deepseek')
  const apiKeyToggleLabel = showApiKey
    ? t('setupPage.forms.apiProvider.apiKey.hide')
    : t('setupPage.forms.apiProvider.apiKey.show')
  const providerPresets = PROVIDER_TEMPLATES.filter((template) => template.id !== 'custom')

  useEffect(() => {
    if (!config.provider_name && !config.base_url) return
    const matched = providerPresets.find(
      (template) => template.name === config.provider_name && template.base_url === config.base_url
    )
    if (matched) {
      setSelectedPreset(matched.id)
      setCustomMode(false)
    } else {
      setCustomMode(true)
    }
  }, [config.base_url, config.provider_name])

  const selectPreset = (presetId: string) => {
    const preset = providerPresets.find((template) => template.id === presetId)
    if (!preset) return
    setSelectedPreset(preset.id)
    onChange({
      ...config,
      provider_name: preset.name,
      base_url: preset.base_url,
      api_key: '',
    })
  }

  const switchToCustom = () => {
    onChange({ provider_name: '', base_url: '', api_key: '' })
    setCustomMode(true)
  }

  const switchToPreset = () => {
    selectPreset(selectedPreset)
    setCustomMode(false)
  }

  return (
    <div className="space-y-6">
      <div className="space-y-3">
        <Label>{t('setupPage.forms.apiProvider.providerName.label')}</Label>
        {/* 表单纵向切换，按钮留在原位，仅线性调整宽度。 */}
        <div className="flex items-center gap-2">
          <div className="min-w-0 flex-1">
            <AnimatePresence initial={false} mode="wait">
              <motion.div
                key={customMode ? 'custom' : 'preset'}
                initial={{ opacity: 0, y: reduceMotion ? 0 : customMode ? 16 : -16 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: reduceMotion ? 0 : customMode ? 16 : -16 }}
                transition={{ duration: reduceMotion ? 0 : 0.16, ease: 'linear' }}
                className="flex gap-2"
              >
                {!customMode ? (
                  <Select value={selectedPreset} onValueChange={selectPreset}>
                    <SelectTrigger aria-label="选择预置提供商" className="h-11 min-w-0 flex-1">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent position="popper" collisionPadding={12}>
                      {providerPresets.map((preset) => (
                        <SelectItem key={preset.id} value={preset.id}>
                          {preset.display_name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : (
                  <>
                    <Input
                      id="provider_name"
                      className="h-11 min-w-0 flex-1"
                      placeholder={t('setupPage.forms.apiProvider.providerName.placeholder')}
                      value={config.provider_name}
                      onChange={(e) => onChange({ ...config, provider_name: e.target.value })}
                    />
                    <Input
                      aria-label={t('setupPage.forms.apiProvider.baseUrl.label')}
                      className="h-11 min-w-0 flex-1 font-mono"
                      placeholder="https://api.example.com/v1"
                      value={config.base_url}
                      onChange={(e) => onChange({ ...config, base_url: e.target.value })}
                    />
                  </>
                )}
              </motion.div>
            </AnimatePresence>
          </div>
          <motion.div
            initial={false}
            animate={{ width: customMode ? 64 : 80 }}
            transition={{ duration: reduceMotion ? 0 : 0.2, ease: 'linear' }}
            className="shrink-0"
          >
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="h-11 w-full"
              onClick={customMode ? switchToPreset : switchToCustom}
            >
              {customMode ? '预置' : '自定义'}
            </Button>
          </motion.div>
        </div>
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
            className="absolute top-1/2 right-2 h-7 w-7 -translate-y-1/2 rounded-md border-0 p-0 hover:bg-transparent"
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
      </div>
    </div>
  )
}

// ====== 步骤4：基础模型配置 ======
interface ModelSetupFormProps {
  config: ModelSetupConfig
  onChange: (config: ModelSetupConfig) => void
  models?: ModelListItem[]
  fetchStatus?: 'idle' | 'loading' | 'success' | 'error'
  fetchError?: string
  providerBaseUrl?: string
}

export function ModelSetupForm({
  config,
  onChange,
  models = [],
  fetchStatus = 'idle',
  fetchError = '',
  providerBaseUrl = '',
}: ModelSetupFormProps) {
  const { t } = useTranslation()

  const providerTemplate = findTemplateByBaseUrl(providerBaseUrl)
  const supportsThinkingSwitch = (identifier: string): boolean => {
    if (!identifier.trim() || (fetchStatus !== 'success' && fetchStatus !== 'error')) return false
    if (providerTemplate?.id === 'deepseek') return true
    const format = resolveThinkingFormatForModel(providerTemplate, identifier)
    return format?.kind === 'enable_thinking' ||
      (format?.kind === 'thinking_type' && format.canDisable === true)
  }

  const inferThinkingEnabled = (modelIdentifier: string) => {
    const normalizedIdentifier = modelIdentifier.trim().toLowerCase()
    return normalizedIdentifier.includes('deepseek-v4-pro')
  }

  const renderModelInput = (role: 'planner' | 'replyer') => {
    const identifier = config[`${role}_model_identifier`]
    const changeModel = (value: string) =>
      onChange({
        ...config,
        [`${role}_model_identifier`]: value,
        [`${role}_model_name`]: value,
        [`${role}_thinking`]: inferThinkingEnabled(value),
      })
    if (fetchStatus === 'success') {
      return (
        <Select value={identifier} onValueChange={changeModel}>
          <SelectTrigger id={`${role}_model_identifier`} className="min-w-0 font-mono">
            <SelectValue placeholder="请选择模型" />
          </SelectTrigger>
          <SelectContent position="popper" collisionPadding={12}>
            {models.map((model) => (
              <SelectItem key={model.id} value={model.id}>{model.id}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      )
    }
    return (
      <Input
        id={`${role}_model_identifier`}
        placeholder={fetchStatus === 'error' ? '手动输入模型标识符' : '填写 API Key 后自动获取'}
        value={fetchStatus === 'error' ? identifier : ''}
        disabled={fetchStatus !== 'error'}
        onChange={(event) => changeModel(event.target.value)}
        className="font-mono"
      />
    )
  }

  return (
    <div className="space-y-6">
      {fetchStatus === 'loading' && (
        <p role="status" className="text-muted-foreground text-sm">正在获取模型列表…</p>
      )}
      {fetchStatus === 'error' && (
        <p role="alert" className="text-destructive text-sm">
          {fetchError}，请手动填写模型标识符。
        </p>
      )}
      <div className="grid min-w-0 md:grid-cols-2">
        <div className="min-w-0 space-y-4 pb-6 md:pr-6 md:pb-0">
          <div className="space-y-3">
            <Label htmlFor="planner_model_identifier" className="flex items-center gap-1">
              {t('setupPage.forms.modelSetup.planner.identifier.label')}
              <HelpTooltip
                content={t('setupPage.forms.modelSetup.planner.identifier.description')}
                side="right"
              />
            </Label>
            {renderModelInput('planner')}
          </div>

          {supportsThinkingSwitch(config.planner_model_identifier) && (
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
          )}
        </div>

        <div className="border-border min-w-0 space-y-4 border-t-2 pt-6 md:border-t-0 md:border-l-2 md:pt-0 md:pl-6">
          <div className="space-y-3">
            <Label htmlFor="replyer_model_identifier" className="flex items-center gap-1">
              {t('setupPage.forms.modelSetup.replyer.identifier.label')}
              <HelpTooltip
                content={t('setupPage.forms.modelSetup.replyer.identifier.description')}
                side="right"
              />
            </Label>
            {renderModelInput('replyer')}
          </div>

          {supportsThinkingSwitch(config.replyer_model_identifier) && (
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
          )}
        </div>
      </div>

    </div>
  )
}
