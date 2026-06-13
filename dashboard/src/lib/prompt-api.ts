import { backendApi, toApiResponse } from '@/lib/http'
import type { ApiResponse } from '@/types/api'

const API_BASE = '/api/webui/config/prompts'

export interface PromptFileInfo {
  name: string
  size: number
  modified_at: number
  display_name: string
  advanced: boolean
  description: string
  customized: boolean
}

export interface PromptCatalog {
  success: boolean
  languages: string[]
  files: Record<string, PromptFileInfo[]>
}

export interface PromptFileContent {
  success: boolean
  language: string
  filename: string
  content: string
  customized: boolean
}

export async function getPromptCatalog(): Promise<ApiResponse<PromptCatalog>> {
  return toApiResponse(() =>
    backendApi.get<PromptCatalog>(API_BASE, {
      errorMessage: '获取 Prompt 文件列表失败',
    })
  )
}

export async function getPromptFile(
  language: string,
  filename: string
): Promise<ApiResponse<PromptFileContent>> {
  return toApiResponse(() =>
    backendApi.get<PromptFileContent>(
      `${API_BASE}/${encodeURIComponent(language)}/${encodeURIComponent(filename)}`,
      {
        errorMessage: '获取 Prompt 文件失败',
      }
    )
  )
}

export async function getDefaultPromptFile(
  language: string,
  filename: string
): Promise<ApiResponse<PromptFileContent>> {
  return toApiResponse(() =>
    backendApi.get<PromptFileContent>(
      `${API_BASE}/${encodeURIComponent(language)}/${encodeURIComponent(filename)}/default`,
      {
        errorMessage: '获取默认 Prompt 文件失败',
      }
    )
  )
}

export async function updatePromptFile(
  language: string,
  filename: string,
  content: string
): Promise<ApiResponse<PromptFileContent>> {
  return toApiResponse(() =>
    backendApi.put<PromptFileContent>(
      `${API_BASE}/${encodeURIComponent(language)}/${encodeURIComponent(filename)}`,
      {
        body: { content },
        errorMessage: '保存 Prompt 文件失败',
      }
    )
  )
}

export async function resetPromptFile(
  language: string,
  filename: string
): Promise<ApiResponse<PromptFileContent>> {
  return toApiResponse(() =>
    backendApi.delete<PromptFileContent>(
      `${API_BASE}/${encodeURIComponent(language)}/${encodeURIComponent(filename)}`,
      {
        errorMessage: '重置 Prompt 文件失败',
      }
    )
  )
}
