'use client'

import { useState, useRef, useEffect } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Send, Loader2, FileText, Menu } from 'lucide-react'
import { chatApi } from '@/lib/api'
import { formatCurrency } from '@/lib/utils'
import ConversationSidebar from '@/components/ConversationSidebar'

interface Message {
  role: 'user' | 'assistant'
  content: string
  sources?: any[]
  metrics?: any
  timestamp: Date
  noDocumentsFound?: boolean
}

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([])
  const [input, setInput] = useState('')
  const [loading, setLoading] = useState(false)
  const [conversationId, setConversationId] = useState<string>()
  const [sidebarOpen, setSidebarOpen] = useState(false)
  const queryClient = useQueryClient();
  const messagesEndRef = useRef<HTMLDivElement>(null)

  // We might want to get the fund ID from the URL or context
  // For now, let's assume no specific fund unless explicitly provided
  const currentFundId: number | null = null; // This would come from URL params or context

  const createNewConversation = async (fundId?: number) => {
    try {
      const conv = await chatApi.createConversation(fundId || currentFundId || undefined);
      const newConvId = conv.conversation_id;
      setConversationId(newConvId);
      // Mark this conversation as already loaded to prevent useEffect from reloading it
      setLastLoadedConversationId(newConvId);
      // Clear messages to start fresh conversation
      setMessages([]);
      return newConvId;
    } catch (error) {
      console.error('Error creating conversation:', error);
      return null;
    }
  }

  const loadConversation = async (id: string) => {
    try {
      const conv = await chatApi.getConversation(id);
      setConversationId(conv.conversation_id);
      
      // Convert the conversation messages to our Message format
      const formattedMessages: Message[] = conv.messages.map((msg: any) => ({
        role: msg.role,
        content: msg.content,
        timestamp: new Date(msg.timestamp)
      }));
      setMessages(formattedMessages);
    } catch (error) {
      console.error('Error loading conversation:', error);
      // If there's an error loading the conversation, clear it
      setConversationId(undefined);
      setMessages([]);
    }
  }

  // Track the last loaded conversation to prevent reloading the same one
  const [lastLoadedConversationId, setLastLoadedConversationId] = useState<string | undefined>();

  useEffect(() => {
    if (conversationId && conversationId !== lastLoadedConversationId) {
      // Only load if this is a different conversation than what's currently loaded
      loadConversation(conversationId);
      setLastLoadedConversationId(conversationId);
    }
  }, [conversationId, lastLoadedConversationId])

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!input.trim() || loading) return

    // Create a new conversation if none exists
    let currentConversationId = conversationId;
    if (!currentConversationId) {
      // You could pass a fundId here if you're in a fund-specific context
      // For example, if this chat page were accessed with a fund context
      currentConversationId = await createNewConversation();
      if (!currentConversationId) {
        return; // If we couldn't create a conversation, exit
      }
    }

    const userMessage: Message = {
      role: 'user',
      content: input,
      timestamp: new Date()
    }

    setMessages(prev => [...prev, userMessage])
    setInput('')
    setLoading(true)

    try {
      // For now, we're not passing a fundId to the query, but it could be added
      // depending on the current context (e.g. if user is viewing a specific fund)
      const response = await chatApi.query(input, undefined, currentConversationId)

      const assistantMessage: Message = {
        role: 'assistant',
        content: response.answer,
        sources: response.sources,
        metrics: response.metrics,
        timestamp: new Date(),
        noDocumentsFound: response.no_documents_found
      }

      setMessages(prev => [...prev, assistantMessage])
      
      // Refresh the conversation list to update title and show the conversation in sidebar
      await queryClient.invalidateQueries({ queryKey: ['conversations'] });
    } catch (error: any) {
      const errorMessage: Message = {
        role: 'assistant',
        content: `Sorry, I encountered an error: ${error.response?.data?.detail || error.message}`,
        timestamp: new Date()
      }
      setMessages(prev => [...prev, errorMessage])
    } finally {
      setLoading(false)
    }
  }

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    // Submit on Enter (without Shift)
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      handleSubmit(e as any)
    }
    // Allow Shift+Enter for new line (default textarea behavior)
  }

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar - Hidden on mobile by default, shown with button */}
      <div className="hidden md:block w-64 border-r bg-white flex-shrink-0">
        <ConversationSidebar
          currentConversationId={conversationId}
          onSelectConversation={handleSelectConversation}
          onCreateNewConversation={createNewConversation}
          onRefreshConversations={() => queryClient.invalidateQueries({ queryKey: ['conversations'] })}
          sidebarOpen={true}
          setSidebarOpen={() => {}} // No need to manage state here since it's always open on desktop
        />
      </div>

      {/* Mobile sidebar overlay */}
      {sidebarOpen && (
        <div className="md:hidden absolute z-20 w-64 border-r bg-white flex-shrink-0 h-full">
          <ConversationSidebar
            currentConversationId={conversationId}
            onSelectConversation={handleSelectConversation}
            onCreateNewConversation={createNewConversation}
            onRefreshConversations={() => queryClient.invalidateQueries({ queryKey: ['conversations'] })}
            sidebarOpen={sidebarOpen}
            setSidebarOpen={setSidebarOpen}
          />
        </div>
      )}

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden max-w-5xl mx-auto w-full">
        {/* Mobile header with menu button */}
        <div className="md:hidden p-4 border-b bg-white flex items-center justify-between">
          <button
            onClick={() => setSidebarOpen(true)}
            className="p-2 rounded-md text-gray-600 hover:bg-gray-100"
          >
            <Menu className="w-5 h-5" />
          </button>
          <h1 className="text-lg font-semibold">Fund Analysis Chat</h1>
          <div className="w-10"></div> {/* Spacer for alignment */}
        </div>

        {/* Chat content */}
        <div className="flex-1 overflow-hidden flex flex-col">
          <div className="p-4 border-b hidden md:block bg-white">
            <div className="flex items-center justify-between max-w-5xl mx-auto w-full">
              <h1 className="text-2xl font-bold">Fund Analysis Chat</h1>
              <p className="text-gray-600 hidden lg:block">Ask questions about fund performance, metrics, and transactions</p>
            </div>
          </div>

          <div className="flex-1 overflow-hidden flex flex-col max-w-5xl mx-auto w-full"> {/* This container for the actual chat */}
            {/* Messages Area */}
            <div className="flex-1 overflow-y-auto p-4 space-y-6 bg-gray-50">
              {messages.length === 0 && (
                <div className="text-center py-12 h-full flex flex-col justify-center">
                  <div className="text-gray-400 mb-4">
                    <FileText className="w-16 h-16 mx-auto" />
                  </div>
                  <h3 className="text-lg font-medium text-gray-900 mb-2">
                    Start a conversation
                  </h3>
                  <p className="text-gray-600 mb-6">
                    Try asking questions like:
                  </p>
                  <div className="space-y-2 max-w-md mx-auto">
                    <SampleQuestion
                      question="What is the current DPI?"
                      onClick={() => setInput("What is the current DPI?")}
                    />
                    <SampleQuestion
                      question="Calculate the IRR for this fund"
                      onClick={() => setInput("Calculate the IRR for this fund")}
                    />
                    <SampleQuestion
                      question="What does Paid-In Capital mean?"
                      onClick={() => setInput("What does Paid-In Capital mean?")}
                    />
                  </div>
                </div>
              )}

              {messages.map((message, index) => (
                <MessageBubble key={index} message={message} />
              ))}

              {loading && (
                <div className="flex items-center space-x-2 text-gray-500">
                  <Loader2 className="w-5 h-5 animate-spin" />
                  <span>Thinking...</span>
                </div>
              )}

              <div ref={messagesEndRef} />
            </div>

            {/* Input Area */}
            <div className="border-t p-4 bg-white m-4 rounded-lg shadow-sm">
              <form onSubmit={handleSubmit} className="flex space-x-2 items-end">
                <div className="flex-1">
                  <textarea
                    value={input}
                    onChange={(e) => setInput(e.target.value)}
                    onKeyDown={handleKeyDown}
                    placeholder="Ask a question about the fund... (Shift+Enter for new line)"
                    className="w-full px-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none"
                    disabled={loading}
                    rows={1}
                    style={{
                      minHeight: '42px',
                      maxHeight: '200px',
                      height: 'auto',
                      overflowY: input.split('\n').length > 5 ? 'auto' : 'hidden'
                    }}
                    onInput={(e) => {
                      const target = e.target as HTMLTextAreaElement
                      target.style.height = 'auto'
                      target.style.height = `${Math.min(target.scrollHeight, 200)}px`
                    }}
                  />
                </div>
                <button
                  type="submit"
                  disabled={loading || !input.trim()}
                  className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:opacity-50 disabled:cursor-not-allowed flex items-center space-x-2 h-[42px]"
                >
                  <Send className="w-4 h-4" />
                  <span>Send</span>
                </button>
              </form>
              <p className="text-xs text-gray-500 mt-2">
                Press <kbd className="px-1 py-0.5 bg-gray-100 border border-gray-300 rounded text-xs">Enter</kbd> to send, <kbd className="px-1 py-0.5 bg-gray-100 border border-gray-300 rounded text-xs">Shift+Enter</kbd> for new line
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  )

  function handleSelectConversation(id: string) {
    setConversationId(id);
  }
}

function MessageBubble({ message }: { message: Message }) {
  const isUser = message.role === 'user'

  return (
    <div className={`flex ${isUser ? 'justify-end' : 'justify-start'}`}>
      <div className={`max-w-3xl ${isUser ? 'ml-12' : 'mr-12'} w-full`}>
        {/* Warning banner for no documents found */}
        {!isUser && message.noDocumentsFound && (
          <div className="mb-3 bg-yellow-50 border border-yellow-200 rounded-lg p-3 flex items-start gap-2">
            <svg className="w-5 h-5 text-yellow-600 mt-0.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
            </svg>
            <div className="text-sm">
              <div className="font-medium text-yellow-900">No relevant documents found</div>
              <div className="text-yellow-700 mt-0.5">
                The system couldn&apos;t find documents matching your query. The response below provides general guidance.
              </div>
            </div>
          </div>
        )}

        <div
          className={`rounded-lg p-4 ${
            isUser
              ? 'bg-blue-600 text-white'
              : 'bg-gray-100 text-gray-900'
          }`}
        >
          <p className="whitespace-pre-wrap">{message.content}</p>
        </div>

        {/* Metrics Display */}
        {message.metrics && (
          <div className="mt-3 bg-white border border-gray-200 rounded-lg p-4">
            <h4 className="font-semibold text-sm text-gray-700 mb-2">Calculated Metrics</h4>
            <div className="grid grid-cols-2 gap-3">
              {Object.entries(message.metrics).map(([key, value]) => {
                if (value === null || value === undefined) return null
                
                let displayValue: string
                if (typeof value === 'number' && key.includes('irr')) {
                  displayValue = `${value.toFixed(2)}%`
                } else if (typeof value === 'number') {
                  displayValue = formatCurrency(value)
                } else {
                  displayValue = String(value)
                }
                
                return (
                  <div key={key} className="text-sm">
                    <span className="text-gray-600">{key.toUpperCase()}:</span>{' '}
                    <span className="font-semibold">{displayValue}</span>
                  </div>
                )
              })}
            </div>
          </div>
        )}

        {/* Sources Display */}
        {message.sources && message.sources.length > 0 && (
          <div className="mt-3">
            <details className="bg-white border border-gray-200 rounded-lg">
              <summary className="px-4 py-2 cursor-pointer text-sm font-medium text-gray-700 hover:bg-gray-50">
                View Sources ({message.sources.length})
              </summary>
              <div className="px-4 py-3 space-y-2 border-t">
                {message.sources.slice(0, 3).map((source, idx) => (
                  <div key={idx} className="text-xs bg-gray-50 p-2 rounded">
                    <p className="text-gray-700 line-clamp-2">{source.content}</p>
                    {source.score && (
                      <p className="text-gray-500 mt-1">
                        Relevance: {(source.score * 100).toFixed(0)}%
                      </p>
                    )}
                  </div>
                ))}
              </div>
            </details>
          </div>
        )}

        <p className="text-xs text-gray-500 mt-2">
          {message.timestamp.toLocaleTimeString()}
        </p>
      </div>
    </div>
  )
}

function SampleQuestion({ question, onClick }: { question: string; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="w-full text-left px-4 py-2 bg-gray-50 hover:bg-gray-100 rounded-lg text-sm text-gray-700 transition"
    >
      &quot;{question}&quot;
    </button>
  )
}
