'use client';

import React from 'react';
import { useQuery } from '@tanstack/react-query';
import { fundApi } from '@/lib/api';
import { formatCurrency, formatDate } from '@/lib/utils';
import { Calendar, Loader2, TrendingDown } from 'lucide-react';
import Pagination from './Pagination';

interface TransactionsTableProps {
  fundId: number;
  type: 'capital_calls' | 'distributions';
}

const TransactionsTable = ({ fundId, type }: TransactionsTableProps) => {
  const [page, setPage] = React.useState(1);
  const limit = 10;

  const { data: transactionsData, isLoading } = useQuery({
    queryKey: ['transactions', fundId, type, page, limit],
    queryFn: () => fundApi.getTransactions(fundId, type, page, limit)
  });

  const transactions = transactionsData?.items || [];
  const total = transactionsData?.total || 0;
  const totalPages = Math.ceil(total / limit);

  const title = type === 'capital_calls' ? 'Capital Calls' : 'Distributions';
  const isCapitalCall = type === 'capital_calls';

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6 flex items-center justify-center h-64">
        <Loader2 className="w-8 h-8 animate-spin text-blue-600" />
      </div>
    );
  }

  if (transactions.length === 0) {
    return (
      <div className="bg-white rounded-lg shadow-md p-6">
        <h2 className="text-xl font-semibold mb-4">{title}</h2>
        <p className="text-gray-500 text-sm">No {type.replace('_', ' ')} found</p>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-lg shadow-md">
      <div className="p-6">
        <h2 className="text-xl font-semibold mb-4">{title}</h2>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Date
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Type
                </th>
                <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Amount
                </th>
                {isCapitalCall ? (
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Call Type
                  </th>
                ) : (
                  <th scope="col" className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Recallable
                  </th>
                )}
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {transactions.map((transaction: any) => (
                <tr key={transaction.id} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    <div className="flex items-center">
                      <Calendar className="w-4 h-4 text-gray-400 mr-2" />
                      {formatDate(transaction.call_date || transaction.distribution_date)}
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {transaction.call_type || transaction.distribution_type}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm font-medium">
                    <span className={isCapitalCall ? 'text-red-600' : 'text-green-600'}>
                      {isCapitalCall ? '-' : '+'}{formatCurrency(Math.abs(transaction.amount))}
                    </span>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {isCapitalCall ? (
                      transaction.call_type
                    ) : (
                      transaction.is_recallable ? (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-100 text-yellow-800">
                          Recallable
                        </span>
                      ) : (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                          Not Recallable
                        </span>
                      )
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      {totalPages > 1 && (
        <Pagination
          currentPage={page}
          totalPages={totalPages}
          onPageChange={setPage}
          showingCount={transactions.length}
          totalCount={total}
        />
      )}
    </div>
  );
};

export default TransactionsTable;