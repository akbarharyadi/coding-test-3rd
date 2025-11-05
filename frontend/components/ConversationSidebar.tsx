'use client';

import { useState, useEffect } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { chatApi } from '@/lib/api';
import { formatDate } from '@/lib/utils';
import { MessageSquare, Plus, Trash2, X } from 'lucide-react';

interface Conversation {
  conversation_id: string;
  fund_id?: number;
  title?: string;
  created_at: string;
  updated_at: string;
}

interface ConversationSidebarProps {
  currentConversationId?: string;
  onSelectConversation: (id: string) => void;
  onCreateNewConversation: () => void;
  onRefreshConversations?: () => void; // Optional function to refresh conversations
  fundId?: number;
  sidebarOpen: boolean;
  setSidebarOpen: (open: boolean) => void;
}

const ConversationSidebar = ({
  currentConversationId,
  onSelectConversation,
  onCreateNewConversation,
  fundId,
  sidebarOpen,
  setSidebarOpen
}: ConversationSidebarProps) => {
  const queryClient = useQueryClient();
  const [deleteConfirmation, setDeleteConfirmation] = useState<string | null>(null);

  const { 
    data: conversations = [], 
    isLoading, 
    refetch 
  } = useQuery({
    queryKey: ['conversations', fundId],
    queryFn: () => chatApi.listConversations(fundId),
    staleTime: 30000, // 30 seconds
    cacheTime: 60000, // 60 seconds
  });

  useEffect(() => {
    if (sidebarOpen) {
      refetch();
    }
  }, [sidebarOpen, refetch]);

  const handleDeleteConversation = async (conversationId: string) => {
    if (deleteConfirmation === conversationId) {
      try {
        await chatApi.deleteConversation(conversationId);
        queryClient.invalidateQueries({ queryKey: ['conversations', fundId] });
        
        // If we deleted the current conversation, create a new one
        if (currentConversationId === conversationId) {
          onCreateNewConversation();
        }
      } catch (error) {
        console.error('Error deleting conversation:', error);
      } finally {
        setDeleteConfirmation(null);
      }
    } else {
      setDeleteConfirmation(conversationId);
    }
  };

  const handleConfirmNewConversation = () => {
    setDeleteConfirmation(null);
    onCreateNewConversation();
  };

  // Format conversation title: either use stored title or create from first message
  const getConversationTitle = (conv: Conversation) => {
    if (conv.title && conv.title.trim() !== '') {
      return conv.title.length > 50 ? conv.title.substring(0, 50) + '...' : conv.title;
    }
    
    // If no specific title, try to generate a title from the first user message in the conversation
    // For now, fall back to date since we don't have the full conversation content here
    return `Chat ${formatDate(conv.created_at)}`;
  };

  return (
    <>
      {/* Mobile overlay */}
      {sidebarOpen && (
        <div 
          className="fixed inset-0 z-20 bg-black bg-opacity-50 md:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}

      {/* Sidebar */}
      <div 
        className={`fixed top-0 left-0 h-full bg-white shadow-lg z-30 w-64 transform transition-transform duration-300 ease-in-out
          ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'} md:translate-x-0 md:static md:z-auto`}
      >
        <div className="flex flex-col h-full">
          {/* Header */}
          <div className="p-4 border-b flex items-center justify-between">
            <h2 className="text-lg font-semibold flex items-center">
              <MessageSquare className="w-5 h-5 mr-2 text-blue-600" />
              Conversations
            </h2>
            <button 
              onClick={handleConfirmNewConversation}
              className="p-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 transition"
              title="New Conversation"
            >
              <Plus className="w-4 h-4" />
            </button>
          </div>

          {/* Close button for mobile */}
          <div className="md:hidden p-3 border-b flex justify-end">
            <button 
              onClick={() => setSidebarOpen(false)}
              className="p-1 rounded hover:bg-gray-100"
            >
              <X className="w-5 h-5" />
            </button>
          </div>

          {/* Conversations List */}
          <div className="flex-1 overflow-y-auto p-2">
            {isLoading ? (
              <div className="p-4 text-center text-gray-500">Loading conversations...</div>
            ) : conversations.length === 0 ? (
              <div className="p-4 text-center text-gray-500">No conversations yet</div>
            ) : (
              <div className="space-y-1">
                {conversations.map((conv) => (
                  <div 
                    key={conv.conversation_id}
                    className={`group p-3 rounded-lg flex items-center justify-between
                      ${currentConversationId === conv.conversation_id 
                        ? 'bg-blue-100 border border-blue-200' 
                        : 'hover:bg-gray-100'}`}
                  >
                    <button
                      onClick={() => {
                        onSelectConversation(conv.conversation_id);
                        setSidebarOpen(false); // Close sidebar on mobile after selection
                      }}
                      className="flex-1 text-left min-w-0 truncate"
                      title={conv.title || `Chat from ${formatDate(conv.created_at)}`}
                    >
                      <div className="font-medium text-sm truncate">
                        {getConversationTitle(conv)}
                      </div>
                      <div className="text-xs text-gray-500">
                        {formatDate(conv.updated_at)}
                      </div>
                    </button>
                    
                    <button
                      onClick={() => handleDeleteConversation(conv.conversation_id)}
                      className="p-1 ml-2 text-gray-400 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity"
                      title="Delete conversation"
                    >
                      {deleteConfirmation === conv.conversation_id ? (
                        <span className="text-xs text-red-600">Confirm?</span>
                      ) : (
                        <Trash2 className="w-4 h-4" />
                      )}
                    </button>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Footer */}
          <div className="p-3 border-t text-xs text-gray-500 text-center">
            {conversations.length} conversation{conversations.length !== 1 ? 's' : ''}
          </div>
        </div>
      </div>
    </>
  );
};

export default ConversationSidebar;