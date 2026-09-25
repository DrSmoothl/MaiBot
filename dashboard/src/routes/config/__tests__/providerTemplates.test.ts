import { describe, expect, it } from 'vitest'

import { findTemplateByBaseUrl, resolveModelFetcherTemplate } from '../providerTemplates'

describe('providerTemplates', () => {
  it('为未知自定义 OpenAI 兼容端点启用模型列表获取', () => {
    const template = resolveModelFetcherTemplate('https://example.com/v1', 'openai')

    expect(template?.id).toBe('custom-openai-compatible')
    expect(template?.display_name).toBe('自定义 OpenAI 兼容端点')
    expect(template?.modelFetcher).toEqual({ endpoint: '/models', parser: 'openai' })
  })

  it('为未知自定义 Gemini 端点使用 Gemini 解析器', () => {
    const template = resolveModelFetcherTemplate(
      'https://generativelanguage.example.com/v1beta',
      'gemini'
    )

    expect(template?.id).toBe('custom-gemini')
    expect(template?.client_type).toBe('gemini')
    expect(template?.modelFetcher).toEqual({ endpoint: '/models', parser: 'gemini' })
  })

  it('为自定义 OpenAI Responses 端点保留 Responses 客户端类型', () => {
    const template = resolveModelFetcherTemplate('https://responses.example.com/v1', 'openai_responses')

    expect(template?.id).toBe('custom-openai-responses')
    expect(template?.client_type).toBe('openai_responses')
    expect(template?.modelFetcher).toEqual({ endpoint: '/models', parser: 'openai' })
  })

  it('未配置 modelFetcher 的内置模板也默认乐观尝试 /models', () => {
    const template = resolveModelFetcherTemplate('https://api.anthropic.com/v1', 'openai')

    expect(template?.id).toBe('anthropic')
    expect(template?.modelFetcher).toEqual({ endpoint: '/models', parser: 'openai' })
  })

  it('Gemini 内置模板未配置 modelFetcher 时按 Gemini 解析器补齐', () => {
    const template = resolveModelFetcherTemplate(
      'https://generativelanguage.googleapis.com/v1beta',
      'gemini'
    )

    expect(template?.id).toBe('gemini')
    expect(template?.modelFetcher).toEqual({ endpoint: '/models', parser: 'gemini' })
  })

  it('直接按 URL 查找模板时不把未知 URL 识别为内置模板', () => {
    expect(findTemplateByBaseUrl('https://example.com/v1')).toBeNull()
  })
})
