'use client'

import { useQuery } from '@tanstack/react-query'
import { useParams } from 'next/navigation'
import { fundApi } from '@/lib/api'
import { formatCurrency, formatPercentage, formatDate } from '@/lib/utils'
import { Loader2, TrendingUp, DollarSign, Calendar } from 'lucide-react'
import FundChart from '@/components/FundChart'
import TransactionsTable from '@/components/TransactionsTable'

export default function FundDetailPage() {
  const params = useParams()
  const fundId = parseInt(params.id as string)

  const { data: fund, isLoading: fundLoading } = useQuery({
    queryKey: ['fund', fundId],
    queryFn: () => fundApi.get(fundId)
  })

  const { data: historicalData, isLoading: historicalLoading } = useQuery({
    queryKey: ['historical', fundId],
    queryFn: () => fundApi.getHistoricalData(fundId),
    enabled: !!fundId
  })

  if (fundLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    )
  }

  if (!fund) {
    return <div>Fund not found</div>
  }

  const metrics = fund.metrics || {}

  // Prepare chart data
  const cumulativeData = historicalData?.cumulative_data || []
  const cumulativeMetrics = historicalData?.cumulative_metrics || []

  return (
    <div className="max-w-7xl mx-auto">
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-4xl font-bold mb-2">{fund.name}</h1>
        <div className="flex items-center space-x-4 text-gray-600">
          {fund.gp_name && <span>GP: {fund.gp_name}</span>}
          {fund.vintage_year && <span>Vintage: {fund.vintage_year}</span>}
          {fund.fund_type && <span>Type: {fund.fund_type}</span>}
        </div>
      </div>

      {/* Metrics Cards */}
      <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
        <MetricCard
          title="DPI"
          value={metrics.dpi?.toFixed(2) + 'x' || 'N/A'}
          description="Distribution to Paid-In"
          icon={<TrendingUp className="w-6 h-6" />}
          color="blue"
        />
        <MetricCard
          title="IRR"
          value={metrics.irr ? formatPercentage(metrics.irr) : 'N/A'}
          description="Internal Rate of Return"
          icon={<TrendingUp className="w-6 h-6" />}
          color="green"
        />
        <MetricCard
          title="Paid-In Capital"
          value={metrics.pic ? formatCurrency(metrics.pic) : 'N/A'}
          description="Total capital called"
          icon={<DollarSign className="w-6 h-6" />}
          color="purple"
        />
        <MetricCard
          title="Distributions"
          value={metrics.total_distributions ? formatCurrency(metrics.total_distributions) : 'N/A'}
          description="Total distributions"
          icon={<DollarSign className="w-6 h-6" />}
          color="orange"
        />
      </div>

      {/* Charts Section */}
      {historicalLoading ? (
        <div className="bg-white rounded-lg shadow-md p-6 mb-8 flex items-center justify-center h-96">
          <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
        </div>
      ) : (
        <>
          {/* Cumulative Capital Flow Chart */}
          {cumulativeData.length > 0 && (
            <div className="mb-8">
              <FundChart
                data={cumulativeData}
                type="composed"
                xKey="date"
                yKeys={['cumulative_paid_in', 'cumulative_distributed']}
                title="Cumulative Capital Flow Over Time"
                colors={['#3b82f6', '#10b981', '#ef4444']}
              />
            </div>
          )}

          {/* DPI and TVPI Over Time Chart */}
          {cumulativeMetrics.length > 0 && (
            <div className="mb-8">
              <FundChart
                data={cumulativeMetrics}
                type="line"
                xKey="date"
                yKeys={['dpi', 'tvpi']}
                title="DPI and TVPI Over Time"
                colors={['#3b82f6', '#f59e0b']}
              />
            </div>
          )}
        </>
      )}

      {/* Transactions Tables with Pagination */}
      <div className="grid lg:grid-cols-2 gap-6">
        {/* Capital Calls */}
        <div>
          <TransactionsTable fundId={fundId} type="capital_calls" />
        </div>

        {/* Distributions */}
        <div>
          <TransactionsTable fundId={fundId} type="distributions" />
        </div>
      </div>
    </div>
  )
}

function MetricCard({ title, value, description, icon, color }: {
  title: string
  value: string
  description: string
  icon: React.ReactNode
  color: 'blue' | 'green' | 'purple' | 'orange'
}) {
  const colorClasses = {
    blue: 'bg-blue-100 text-blue-600',
    green: 'bg-green-100 text-green-600',
    purple: 'bg-purple-100 text-purple-600',
    orange: 'bg-orange-100 text-orange-600',
  }

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      <div className={`w-12 h-12 rounded-lg ${colorClasses[color]} flex items-center justify-center mb-4`}>
        {icon}
      </div>
      <h3 className="text-sm font-medium text-gray-600 mb-1">{title}</h3>
      <p className="text-2xl font-bold text-gray-900 mb-1">{value}</p>
      <p className="text-xs text-gray-500">{description}</p>
    </div>
  )
}
