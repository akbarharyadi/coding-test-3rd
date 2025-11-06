'use client'

import { useState, useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { fundApi } from '@/lib/api'
import { formatCurrency } from '@/lib/utils'
import { ArrowUp, ArrowDown, Minus, TrendingUp, Award } from 'lucide-react'

interface Fund {
  id: number
  name: string
  gp_name?: string
  vintage_year?: number
}

interface FundComparison {
  fund_id: number
  fund_name: string
  gp_name?: string
  vintage_year?: number
  metrics: {
    dpi?: number
    tvpi?: number
    irr?: number
    moic?: number
    rvpi?: number
    pic?: number  // Paid-In Capital
    total_distributions?: number  // Total Distributions
    nav?: number
  }
  rankings?: {
    dpi?: number
    tvpi?: number
    irr?: number
    moic?: number
    rvpi?: number
  }
  capital_calls_count: number
  distributions_count: number
}

export default function ComparePage() {
  const [selectedFundIds, setSelectedFundIds] = useState<number[]>([])
  const [comparisonData, setComparisonData] = useState<FundComparison[] | null>(null)

  // Fetch all funds for selection
  const { data: funds, isLoading: fundsLoading } = useQuery<Fund[]>({
    queryKey: ['funds'],
    queryFn: fundApi.list,
  })

  // Fetch comparison data when funds are selected
  const { data: comparison, isLoading: comparisonLoading } = useQuery({
    queryKey: ['compare', selectedFundIds],
    queryFn: () => fundApi.compare(selectedFundIds),
    enabled: selectedFundIds.length >= 2,
  })

  useEffect(() => {
    if (comparison) {
      setComparisonData(comparison.funds)
    }
  }, [comparison])

  const toggleFund = (fundId: number) => {
    setSelectedFundIds(prev =>
      prev.includes(fundId)
        ? prev.filter(id => id !== fundId)
        : prev.length < 10
          ? [...prev, fundId]
          : prev // Max 10 funds
    )
  }

  const getRankIcon = (rank?: number) => {
    if (!rank) return null
    if (rank === 1) return <Award className="w-4 h-4 text-yellow-500 inline ml-1" />
    if (rank === 2) return <Award className="w-4 h-4 text-gray-400 inline ml-1" />
    if (rank === 3) return <Award className="w-4 h-4 text-amber-600 inline ml-1" />
    return null
  }

  const formatMetric = (value: number | undefined | null, type: 'currency' | 'percentage' | 'number' = 'number') => {
    if (value === undefined || value === null) return 'N/A'

    switch (type) {
      case 'currency':
        return formatCurrency(value)
      case 'percentage':
        return `${(value * 100).toFixed(2)}%`
      default:
        return value.toFixed(2)
    }
  }

  return (
    <div className="container mx-auto p-6">
      <div className="mb-8">
        <h1 className="text-3xl font-bold mb-2">Fund Comparison</h1>
        <p className="text-gray-600">Compare multiple funds side-by-side to analyze their performance</p>
      </div>

      {/* Fund Selection */}
      <div className="bg-white rounded-lg shadow p-6 mb-6">
        <h2 className="text-xl font-semibold mb-4">Select Funds to Compare</h2>
        <p className="text-sm text-gray-600 mb-4">
          Choose between 2 and 10 funds. Selected: {selectedFundIds.length}
        </p>

        {fundsLoading ? (
          <p>Loading funds...</p>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
            {funds?.map(fund => (
              <button
                key={fund.id}
                onClick={() => toggleFund(fund.id)}
                className={`p-4 rounded-lg border-2 text-left transition ${
                  selectedFundIds.includes(fund.id)
                    ? 'border-blue-500 bg-blue-50'
                    : 'border-gray-200 hover:border-gray-300'
                }`}
              >
                <div className="font-semibold">{fund.name}</div>
                {fund.gp_name && (
                  <div className="text-sm text-gray-600">{fund.gp_name}</div>
                )}
                {fund.vintage_year && (
                  <div className="text-xs text-gray-500">Vintage: {fund.vintage_year}</div>
                )}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Comparison Table */}
      {selectedFundIds.length >= 2 && (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          {comparisonLoading ? (
            <div className="p-8 text-center">
              <p>Loading comparison...</p>
            </div>
          ) : comparisonData ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider sticky left-0 bg-gray-50 z-10">
                      Metric
                    </th>
                    {comparisonData.map(fund => (
                      <th key={fund.fund_id} className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider min-w-[200px]">
                        <div className="font-bold text-gray-900">{fund.fund_name}</div>
                        {fund.gp_name && <div className="text-gray-600 font-normal">{fund.gp_name}</div>}
                        {fund.vintage_year && <div className="text-gray-500 font-normal">Vintage {fund.vintage_year}</div>}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {/* Paid-In Capital */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      Paid-In Capital
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {formatMetric(fund.metrics.pic, 'currency')}
                      </td>
                    ))}
                  </tr>

                  {/* Distributed Capital */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      Distributed Capital
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {formatMetric(fund.metrics.total_distributions, 'currency')}
                      </td>
                    ))}
                  </tr>

                  {/* NAV */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      NAV
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {formatMetric(fund.metrics.nav, 'currency')}
                      </td>
                    ))}
                  </tr>

                  {/* Section Header: Performance Metrics */}
                  <tr className="bg-blue-50">
                    <td colSpan={comparisonData.length + 1} className="px-6 py-3 text-sm font-bold text-blue-900">
                      Performance Metrics
                    </td>
                  </tr>

                  {/* DPI */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      DPI (Distributed to Paid-In)
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm">
                        <span className={fund.rankings?.dpi === 1 ? 'font-bold text-green-600' : 'text-gray-900'}>
                          {formatMetric(fund.metrics.dpi)}
                          {getRankIcon(fund.rankings?.dpi)}
                        </span>
                      </td>
                    ))}
                  </tr>

                  {/* TVPI */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      TVPI (Total Value to Paid-In)
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm">
                        <span className={fund.rankings?.tvpi === 1 ? 'font-bold text-green-600' : 'text-gray-900'}>
                          {formatMetric(fund.metrics.tvpi)}
                          {getRankIcon(fund.rankings?.tvpi)}
                        </span>
                      </td>
                    ))}
                  </tr>

                  {/* IRR */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      IRR (Internal Rate of Return)
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm">
                        <span className={fund.rankings?.irr === 1 ? 'font-bold text-green-600' : 'text-gray-900'}>
                          {formatMetric(fund.metrics.irr, 'percentage')}
                          {getRankIcon(fund.rankings?.irr)}
                        </span>
                      </td>
                    ))}
                  </tr>

                  {/* MOIC */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      MOIC (Multiple on Invested Capital)
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm">
                        <span className={fund.rankings?.moic === 1 ? 'font-bold text-green-600' : 'text-gray-900'}>
                          {formatMetric(fund.metrics.moic)}
                          {getRankIcon(fund.rankings?.moic)}
                        </span>
                      </td>
                    ))}
                  </tr>

                  {/* RVPI */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      RVPI (Residual Value to Paid-In)
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm">
                        <span className={fund.rankings?.rvpi === 1 ? 'font-bold text-green-600' : 'text-gray-900'}>
                          {formatMetric(fund.metrics.rvpi)}
                          {getRankIcon(fund.rankings?.rvpi)}
                        </span>
                      </td>
                    ))}
                  </tr>

                  {/* Section Header: Activity */}
                  <tr className="bg-blue-50">
                    <td colSpan={comparisonData.length + 1} className="px-6 py-3 text-sm font-bold text-blue-900">
                      Activity
                    </td>
                  </tr>

                  {/* Capital Calls Count */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      Capital Calls
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {fund.capital_calls_count}
                      </td>
                    ))}
                  </tr>

                  {/* Distributions Count */}
                  <tr className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900 sticky left-0 bg-white">
                      Distributions
                    </td>
                    {comparisonData.map(fund => (
                      <td key={fund.fund_id} className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {fund.distributions_count}
                      </td>
                    ))}
                  </tr>
                </tbody>
              </table>
            </div>
          ) : null}
        </div>
      )}

      {selectedFundIds.length === 1 && (
        <div className="bg-yellow-50 border border-yellow-200 rounded-lg p-4 text-center">
          <p className="text-yellow-800">Please select at least one more fund to compare</p>
        </div>
      )}

      {selectedFundIds.length === 0 && (
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-8 text-center">
          <TrendingUp className="w-16 h-16 text-blue-400 mx-auto mb-4" />
          <h3 className="text-lg font-medium text-blue-900 mb-2">Start Comparing Funds</h3>
          <p className="text-blue-700">Select at least 2 funds from above to see a detailed comparison</p>
        </div>
      )}
    </div>
  )
}
