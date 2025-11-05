'use client';

import {
  LineChart,
  Line,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
  ComposedChart,
  Area
} from 'recharts';

interface ChartDataPoint {
  date: string;
  [key: string]: number | string;
}

interface FundChartProps {
  data: ChartDataPoint[];
  type: 'line' | 'bar' | 'area' | 'composed';
  xKey: string;
  yKeys: string[];
  title?: string;
  colors?: string[];
}

const FundChart = ({
  data,
  type = 'line',
  xKey,
  yKeys,
  title,
  colors = ['#3b82f6', '#10b981', '#ef4444', '#f59e0b', '#8b5cf6']
}: FundChartProps) => {
  const renderChart = () => {
    const chartProps = {
      data,
      margin: { top: 10, right: 30, left: 20, bottom: 20 }
    };

    const renderAxes = () => (
      <>
        <XAxis dataKey={xKey} tick={{ fontSize: 12 }} />
        <YAxis tick={{ fontSize: 12 }} />
        <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
        <Tooltip
          contentStyle={{ backgroundColor: 'white', borderRadius: '0.5rem', border: '1px solid #e5e7eb' }}
          formatter={(value) => typeof value === 'number' ? [`$${value.toLocaleString()}`, ''] : [value, '']}
          labelStyle={{ fontWeight: 'bold', color: '#374151' }}
        />
        <Legend />
      </>
    );

    if (type === 'line') {
      return (
        <ResponsiveContainer width="100%" height={400}>
          <LineChart {...chartProps}>
            {renderAxes()}
            {yKeys.map((yKey, index) => (
              <Line
                key={yKey}
                type="monotone"
                dataKey={yKey}
                stroke={colors[index % colors.length]}
                strokeWidth={2}
                dot={{ r: 4 }}
                activeDot={{ r: 6, stroke: colors[index % colors.length], strokeWidth: 2 }}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      );
    }

    if (type === 'bar') {
      return (
        <ResponsiveContainer width="100%" height={400}>
          <BarChart {...chartProps}>
            {renderAxes()}
            {yKeys.map((yKey, index) => (
              <Bar
                key={yKey}
                dataKey={yKey}
                fill={colors[index % colors.length]}
                radius={[4, 4, 0, 0]}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      );
    }

    if (type === 'area') {
      return (
        <ResponsiveContainer width="100%" height={400}>
          <AreaChart {...chartProps}>
            {renderAxes()}
            {yKeys.map((yKey, index) => (
              <Area
                key={yKey}
                type="monotone"
                dataKey={yKey}
                fill={colors[index % colors.length]}
                fillOpacity={0.6}
                stroke={colors[index % colors.length]}
                strokeWidth={2}
              />
            ))}
          </AreaChart>
        </ResponsiveContainer>
      );
    }

    // Composed chart (default)
    return (
      <ResponsiveContainer width="100%" height={400}>
        <ComposedChart {...chartProps}>
          {renderAxes()}
          {yKeys.map((yKey, index) => {
            // For the first item, render as line; for others, render as bars
            return index === 0 ? (
              <Line
                key={yKey}
                type="monotone"
                dataKey={yKey}
                stroke={colors[index % colors.length]}
                strokeWidth={2}
                dot={{ r: 4 }}
                activeDot={{ r: 6, stroke: colors[index % colors.length], strokeWidth: 2 }}
              />
            ) : (
              <Bar
                key={yKey}
                dataKey={yKey}
                fill={colors[index % colors.length]}
                radius={[4, 4, 0, 0]}
              />
            );
          })}
        </ComposedChart>
      </ResponsiveContainer>
    );
  };

  return (
    <div className="bg-white rounded-lg shadow-md p-6">
      {title && <h3 className="text-lg font-semibold mb-4 text-gray-900">{title}</h3>}
      <div className="h-[400px] w-full">
        {renderChart()}
      </div>
    </div>
  );
};

export default FundChart;