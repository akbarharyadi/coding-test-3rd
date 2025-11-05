'use client'

import { useState, Suspense } from 'react'
import { useSearchParams, useRouter } from 'next/navigation'
import SearchInterface from '@/components/SearchInterface'
import { SearchResult } from '@/lib/api'

function SearchPageContent() {
  const router = useRouter()
  const searchParams = useSearchParams()
  const initialQuery = searchParams?.get('q') || ''
  const initialFundId = searchParams?.get('fund_id')

  const [selectedResult, setSelectedResult] = useState<SearchResult | null>(null)

  const handleResultClick = (result: SearchResult) => {
    setSelectedResult(result)
    // Optionally navigate to document detail page
    // if (result.metadata.document_id) {
    //   router.push(`/documents/${result.metadata.document_id}`)
    // }
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <div className="bg-white border-b border-gray-200">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-6">
          <h1 className="text-3xl font-bold text-gray-900">Semantic Search</h1>
          <p className="mt-2 text-gray-600">
            Search documents by meaning, not just keywords
          </p>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          {/* Search Interface */}
          <div className="lg:col-span-2">
            <SearchInterface
              defaultFundId={initialFundId ? parseInt(initialFundId) : undefined}
              onResultClick={handleResultClick}
            />
          </div>

          {/* Sidebar */}
          <div className="space-y-6">
            {/* Search Tips */}
            <div className="bg-white rounded-lg border border-gray-200 p-6">
              <h3 className="font-semibold text-gray-900 mb-4">Search Tips</h3>
              <ul className="space-y-3 text-sm text-gray-600">
                <li className="flex items-start gap-2">
                  <span className="text-blue-500 font-bold">•</span>
                  <span>Use natural language like &ldquo;capital call in Q4&rdquo;</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-blue-500 font-bold">•</span>
                  <span>Search by meaning, not just keywords</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-blue-500 font-bold">•</span>
                  <span>Ask questions like &ldquo;What is DPI?&rdquo;</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-blue-500 font-bold">•</span>
                  <span>Results show similarity scores</span>
                </li>
                <li className="flex items-start gap-2">
                  <span className="text-blue-500 font-bold">•</span>
                  <span>System automatically finds the best matches</span>
                </li>
              </ul>
            </div>

            {/* How It Works */}
            <div className="bg-blue-50 rounded-lg border border-blue-200 p-6">
              <h3 className="font-semibold text-blue-900 mb-3">How It Works</h3>
              <div className="space-y-3 text-sm text-blue-800">
                <p>
                  Semantic search uses AI to understand the <strong>meaning</strong> of your query,
                  not just keywords.
                </p>
                <p>
                  Ask questions naturally, like &ldquo;What was the Q4 distribution?&rdquo; instead of
                  searching for exact phrases.
                </p>
                <p>
                  The system finds relevant documents even if they use different words to
                  express the same concept.
                </p>
              </div>
            </div>

            {/* Selected Result Details */}
            {selectedResult && (
              <div className="bg-white rounded-lg border border-gray-200 p-6">
                <h3 className="font-semibold text-gray-900 mb-4">Selected Result</h3>
                <dl className="space-y-2 text-sm">
                  <div>
                    <dt className="font-medium text-gray-700">Similarity Score</dt>
                    <dd className="text-gray-600">{(selectedResult.score * 100).toFixed(1)}%</dd>
                  </div>
                  {(selectedResult.metadata.document_title || selectedResult.metadata.document_id) && (
                    <div>
                      <dt className="font-medium text-gray-700">Document</dt>
                      <dd className="text-gray-600">
                        {selectedResult.metadata.document_title || `Document #${selectedResult.metadata.document_id}`}
                      </dd>
                    </div>
                  )}
                  {(selectedResult.metadata.fund_name || selectedResult.metadata.fund_id) && (
                    <div>
                      <dt className="font-medium text-gray-700">Fund</dt>
                      <dd className="text-gray-600">
                        {selectedResult.metadata.fund_name || `Fund #${selectedResult.metadata.fund_id}`}
                      </dd>
                    </div>
                  )}
                  {selectedResult.metadata.page_number && (
                    <div>
                      <dt className="font-medium text-gray-700">Page Number</dt>
                      <dd className="text-gray-600">{selectedResult.metadata.page_number}</dd>
                    </div>
                  )}
                  <div>
                    <dt className="font-medium text-gray-700">Source Backend</dt>
                    <dd className="text-gray-600 capitalize">{selectedResult.source}</dd>
                  </div>
                </dl>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}

export default function SearchPage() {
  return (
    <Suspense fallback={<div className="p-8 text-center">Loading search...</div>}>
      <SearchPageContent />
    </Suspense>
  )
}
