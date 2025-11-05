import axios from 'axios'

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

export const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Document APIs
export const documentApi = {
  upload: async (file: File, fundId?: number) => {
    const formData = new FormData()
    formData.append('file', file)
    if (fundId) {
      formData.append('fund_id', fundId.toString())
    }
    
    const response = await api.post('/api/documents/upload', formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    })
    return response.data
  },
  
  getStatus: async (documentId: number) => {
    const response = await api.get(`/api/documents/${documentId}/status`)
    return response.data
  },
  
  list: async (fundId?: number) => {
    const params = fundId ? { fund_id: fundId } : {}
    const response = await api.get('/api/documents/', { params })
    return response.data
  },
  
  delete: async (documentId: number) => {
    const response = await api.delete(`/api/documents/${documentId}`)
    return response.data
  },
}

// Fund APIs
export const fundApi = {
  list: async () => {
    const response = await api.get('/api/funds/')
    return response.data
  },
  
  get: async (fundId: number) => {
    const response = await api.get(`/api/funds/${fundId}`)
    return response.data
  },
  
  create: async (fund: any) => {
    const response = await api.post('/api/funds/', fund)
    return response.data
  },
  
  getTransactions: async (fundId: number, type: string, page: number = 1, limit: number = 50) => {
    const response = await api.get(`/api/funds/${fundId}/transactions`, {
      params: { transaction_type: type, page, limit }
    })
    return response.data
  },
  
  getMetrics: async (fundId: number) => {
    const response = await api.get(`/api/funds/${fundId}/metrics`)
    return response.data
  },
}

// Chat APIs
export const chatApi = {
  query: async (query: string, fundId?: number, conversationId?: string) => {
    const response = await api.post('/api/chat/query', {
      query,
      fund_id: fundId,
      conversation_id: conversationId,
    })
    return response.data
  },
  
  createConversation: async (fundId?: number) => {
    const response = await api.post('/api/chat/conversations', {
      fund_id: fundId,
    })
    return response.data
  },
  
  getConversation: async (conversationId: string) => {
    const response = await api.get(`/api/chat/conversations/${conversationId}`)
    return response.data
  },
}

// Metrics APIs
export const metricsApi = {
  getFundMetrics: async (fundId: number, metric?: string) => {
    const params = metric ? { metric } : {}
    const response = await api.get(`/api/metrics/funds/${fundId}/metrics`, { params })
    return response.data
  },
}

// Search APIs
export interface SearchRequest {
  query: string
  k?: number
  fund_id?: number
  document_id?: number
  backend?: 'postgresql' | 'faiss' | 'hybrid'
  include_content?: boolean
}

export interface SearchResult {
  content?: string
  metadata: {
    document_id?: number
    document_title?: string
    fund_id?: number
    fund_name?: string
    page_number?: number
    offset_start?: number
    offset_end?: number
    [key: string]: any
  }
  score: number
  source: string
}

export interface SearchResponse {
  results: SearchResult[]
  total: number
  query: string
  backend_used: string
  processing_time?: number
}

export interface SearchStats {
  available_backends: string[]
  preferred_backend: string
  faiss_available: boolean
  faiss_vectors?: number
  postgresql_available: boolean
}

export const searchApi = {
  // Perform semantic search
  search: async (request: SearchRequest): Promise<SearchResponse> => {
    const response = await api.post('/api/search/', request)
    return response.data
  },

  // Convenience method for simple searches
  simpleSearch: async (
    query: string,
    options?: {
      k?: number
      fundId?: number
      documentId?: number
      backend?: 'postgresql' | 'faiss' | 'hybrid'
    }
  ): Promise<SearchResponse> => {
    const params: any = { query }
    if (options?.k) params.k = options.k
    if (options?.fundId) params.fund_id = options.fundId
    if (options?.documentId) params.document_id = options.documentId
    if (options?.backend) params.backend = options.backend

    const response = await api.get('/api/search/', { params })
    return response.data
  },

  // Get search service statistics
  getStats: async (): Promise<SearchStats> => {
    const response = await api.get('/api/search/stats')
    return response.data
  },

  // Rebuild FAISS index
  rebuildIndex: async (fundId?: number): Promise<{ message: string; vectors_indexed: number; fund_id?: number }> => {
    const params = fundId ? { fund_id: fundId } : {}
    const response = await api.post('/api/search/rebuild-index', null, { params })
    return response.data
  },
}
