'use client'

import { useState } from 'react'
import { Search, Loader2, AlertCircle, Database, Zap, GitMerge } from 'lucide-react'
import { searchApi, SearchResult, SearchStats } from '@/lib/api'

interface SearchInterfaceProps {
  defaultFundId?: number
  onResultClick?: (result: SearchResult) => void
}

export default function SearchInterface({ defaultFundId, onResultClick }: SearchInterfaceProps) {
  const [query, setQuery] = useState('')
  const [results, setResults] = useState<SearchResult[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [stats, setStats] = useState<SearchStats | null>(null)
  const [processingTime, setProcessingTime] = useState<number | null>(null)
  const [backendUsed, setBackendUsed] = useState<string | null>(null)
  const [hasSearched, setHasSearched] = useState(false)

  // Search options
  const [k, setK] = useState(5)
  const [fundId, setFundId] = useState<number | undefined>(defaultFundId)
  const [backend, setBackend] = useState<'postgresql' | 'faiss' | 'hybrid' | 'auto'>('auto')

  // Load stats on mount
  useState(() => {
    searchApi.getStats().then(setStats).catch(console.error)
  })

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!query.trim()) {
      setError('Please enter a search query')
      return
    }

    setLoading(true)
    setError(null)
    setHasSearched(true)

    try {
      const response = await searchApi.search({
        query: query.trim(),
        k,
        fund_id: fundId,
        backend: backend === 'auto' ? undefined : backend,
        include_content: true,
      })

      setResults(response.results)
      setProcessingTime(response.processing_time || null)
      setBackendUsed(response.backend_used)

      if (response.results.length === 0) {
        setError('No results found. Try a different search term.')
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Search failed. Please try again.')
      setResults([])
    } finally {
      setLoading(false)
    }
  }

  const getBackendIcon = (backend: string) => {
    switch (backend.toLowerCase()) {
      case 'faiss':
        return <Zap className="h-4 w-4 text-yellow-500" />
      case 'postgresql':
        return <Database className="h-4 w-4 text-blue-500" />
      case 'hybrid':
        return <GitMerge className="h-4 w-4 text-purple-500" />
      default:
        return <Database className="h-4 w-4" />
    }
  }

  const highlightText = (text: string, query: string) => {
    if (!query.trim()) return text

    const parts = text.split(new RegExp(`(${query})`, 'gi'))
    return (
      <span>
        {parts.map((part, i) =>
          part.toLowerCase() === query.toLowerCase() ? (
            <mark key={i} className="bg-yellow-200 font-semibold">{part}</mark>
          ) : (
            <span key={i}>{part}</span>
          )
        )}
      </span>
    )
  }

  return (
    <div className="w-full max-w-4xl mx-auto space-y-6">
      {/* Search Form */}
      <form onSubmit={handleSearch} className="space-y-4">
        <div className="relative">
          <Search className="absolute left-3 top-3 h-5 w-5 text-gray-400" />
          <input
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Search documents by meaning... (e.g., 'capital call Q4 2023')"
            className="w-full pl-10 pr-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
            disabled={loading}
          />
        </div>

        {/* Simple Options */}
        <div className="flex items-center gap-4 text-sm text-gray-600">
          <span>Show</span>
          <select
            value={k}
            onChange={(e) => setK(parseInt(e.target.value))}
            className="px-3 py-2 border border-gray-300 rounded-md focus:ring-2 focus:ring-blue-500"
            disabled={loading}
          >
            <option value="3">3</option>
            <option value="5">5</option>
            <option value="10">10</option>
            <option value="20">20</option>
            <option value="50">50</option>
          </select>
          <span>results</span>
        </div>

        <button
          type="submit"
          disabled={loading || !query.trim()}
          className="w-full py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed flex items-center justify-center gap-2 font-medium transition-colors"
        >
          {loading ? (
            <>
              <Loader2 className="h-5 w-5 animate-spin" />
              Searching...
            </>
          ) : (
            <>
              <Search className="h-5 w-5" />
              Search
            </>
          )}
        </button>
      </form>

      {/* Error Message */}
      {error && (
        <div className="bg-red-50 border border-red-200 rounded-lg p-4 flex items-start gap-3">
          <AlertCircle className="h-5 w-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="font-medium text-red-900">Search Error</h3>
            <p className="text-sm text-red-700 mt-1">{error}</p>
          </div>
        </div>
      )}

      {/* Results */}
      {results.length > 0 && (
        <div className="space-y-3">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold text-gray-900">
              {results.length} result{results.length !== 1 ? 's' : ''} found
            </h2>
            {backendUsed && (
              <div className="flex items-center gap-2 text-sm text-gray-600">
                {getBackendIcon(backendUsed)}
                <span>{backendUsed}</span>
              </div>
            )}
          </div>

          {results.map((result, index) => (
            <div
              key={index}
              onClick={() => onResultClick?.(result)}
              className={`border border-gray-200 rounded-lg p-4 hover:border-blue-300 transition-colors ${
                onResultClick ? 'cursor-pointer' : ''
              }`}
            >
              <div className="flex items-start justify-between gap-4">
                <div className="flex-1 min-w-0">
                  {/* Metadata */}
                  <div className="flex items-center gap-3 text-sm text-gray-600 mb-2 flex-wrap">
                    {result.metadata.document_title && (
                      <span className="font-medium">{result.metadata.document_title}</span>
                    )}
                    {!result.metadata.document_title && result.metadata.document_id && (
                      <span>Document #{result.metadata.document_id}</span>
                    )}
                    {result.metadata.fund_name && (
                      <span>{result.metadata.fund_name}</span>
                    )}
                    {!result.metadata.fund_name && result.metadata.fund_id && (
                      <span>Fund #{result.metadata.fund_id}</span>
                    )}
                    {result.metadata.page_number && (
                      <span>Page {result.metadata.page_number}</span>
                    )}
                  </div>

                  {/* Content */}
                  {result.content && (
                    <p className="text-gray-800 leading-relaxed">
                      {highlightText(
                        result.content.length > 300
                          ? result.content.substring(0, 300) + '...'
                          : result.content,
                        query
                      )}
                    </p>
                  )}
                </div>

                {/* Score */}
                <div className="flex-shrink-0 text-right">
                  <div className="text-sm font-medium text-gray-900">
                    {(result.score * 100).toFixed(1)}%
                  </div>
                  <div className="text-xs text-gray-500">similarity</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Empty State - Only show after search */}
      {!loading && !error && results.length === 0 && hasSearched && (
        <div className="text-center py-12 text-gray-500">
          <Search className="h-12 w-12 mx-auto mb-4 text-gray-400" />
          <p className="text-lg font-medium mb-2">No results found</p>
          <p className="text-sm">Try a different search term</p>
        </div>
      )}

      
      {/* Time taken */}
      {stats && (
        <div className="bg-gray-50 border border-gray-200 rounded-lg p-4 text-sm">
          <div className="flex items-center justify-between flex-wrap gap-2">
            {processingTime && (
              <span className="text-gray-500">
                Time taken: <strong>{processingTime.toFixed(2)}s</strong>
              </span>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
