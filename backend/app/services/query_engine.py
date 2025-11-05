"""
Query engine service for RAG-based question answering
"""
from typing import Dict, Any, List, Optional
import time
import logging
from langchain_openai import ChatOpenAI
from langchain_community.llms import Ollama
from langchain.prompts import ChatPromptTemplate
from app.core.config import settings
from app.services.vector_store import VectorStore
from app.services.metrics_calculator import MetricsCalculator
from app.services.cache_service import cache_service
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class QueryEngine:
    """RAG-based query engine for fund analysis"""

    def __init__(self, db: Session, use_cache: bool = True):
        self.db = db
        self.vector_store = VectorStore()
        self.metrics_calculator = MetricsCalculator(db)
        self.llm = self._initialize_llm()
        self.use_cache = use_cache
    
    def _initialize_llm(self):
        """Initialize LLM"""
        if settings.OPENAI_API_KEY:
            return ChatOpenAI(
                model=settings.OPENAI_MODEL,
                temperature=0,
                openai_api_key=settings.OPENAI_API_KEY
            )
        else:
            # Fallback to local LLM
            return Ollama(model="llama3.2-3b-fast")
    
    async def process_query(
        self,
        query: str,
        fund_id: Optional[int] = None,
        conversation_history: List[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Process a user query using RAG with caching

        Args:
            query: User question
            fund_id: Optional fund ID for context
            conversation_history: Previous conversation messages

        Returns:
            Response with answer, sources, and metrics
        """
        start_time = time.time()

        # Check cache first (only for queries without conversation history)
        if self.use_cache and not conversation_history:
            cached_result = cache_service.get_query_cache(query, fund_id)
            if cached_result:
                logger.info(f"Cache hit for query: {query[:50]}...")
                cached_result["processing_time"] = round(time.time() - start_time, 2)
                cached_result["cached"] = True
                return cached_result

        # Step 1: Classify query intent
        intent = await self._classify_intent(query)
        
        # Step 2: Retrieve relevant context from vector store
        filter_metadata = {"fund_id": fund_id} if fund_id else None
        relevant_docs = await self.vector_store.similarity_search(
            query=query,
            k=settings.TOP_K_RESULTS,
            filter_metadata=filter_metadata
        )
        
        # Step 3: Calculate metrics if needed
        metrics = None
        if intent == "calculation" and fund_id:
            metrics = self.metrics_calculator.calculate_all_metrics(fund_id)
        
        # Step 4: Check if documents were found
        no_documents_found = len(relevant_docs) == 0

        # Step 5: Generate response using LLM
        answer = await self._generate_response(
            query=query,
            context=relevant_docs,
            metrics=metrics,
            conversation_history=conversation_history or [],
            no_documents_found=no_documents_found
        )

        processing_time = time.time() - start_time

        result = {
            "answer": answer,
            "sources": [
                {
                    "content": doc["content"],
                    "metadata": {
                        k: v for k, v in doc.items()
                        if k not in ["content", "score"]
                    },
                    "score": doc.get("score")
                }
                for doc in relevant_docs
            ],
            "metrics": metrics,
            "processing_time": round(processing_time, 2),
            "no_documents_found": no_documents_found,
            "cached": False
        }

        # Cache result (only for queries without conversation history)
        if self.use_cache and not conversation_history:
            cache_service.set_query_cache(query, result, fund_id, ttl=3600)
            logger.info(f"Cached query result: {query[:50]}...")

        return result
    
    async def _classify_intent(self, query: str) -> str:
        """
        Classify query intent
        
        Returns:
            'calculation', 'definition', 'retrieval', or 'general'
        """
        query_lower = query.lower()
        
        # Calculation keywords
        calc_keywords = [
            "calculate", "what is the", "current", "dpi", "irr", "tvpi", 
            "rvpi", "pic", "paid-in capital", "return", "performance"
        ]
        if any(keyword in query_lower for keyword in calc_keywords):
            return "calculation"
        
        # Definition keywords
        def_keywords = [
            "what does", "mean", "define", "explain", "definition", 
            "what is a", "what are"
        ]
        if any(keyword in query_lower for keyword in def_keywords):
            return "definition"
        
        # Retrieval keywords
        ret_keywords = [
            "show me", "list", "all", "find", "search", "when", 
            "how many", "which"
        ]
        if any(keyword in query_lower for keyword in ret_keywords):
            return "retrieval"
        
        return "general"
    
    async def _generate_response(
        self,
        query: str,
        context: List[Dict[str, Any]],
        metrics: Optional[Dict[str, Any]],
        conversation_history: List[Dict[str, str]],
        no_documents_found: bool = False
    ) -> str:
        """Generate response using LLM"""

        # Build context string
        if no_documents_found:
            context_str = "[No relevant documents found in the database]"
        else:
            context_str = "\n\n".join([
                f"[Source {i+1}]\n{doc['content']}"
                for i, doc in enumerate(context[:3])  # Use top 3 sources
            ])
        
        # Build metrics string
        metrics_str = ""
        if metrics:
            metrics_str = "\n\nAvailable Metrics:\n"
            for key, value in metrics.items():
                if value is not None:
                    metrics_str += f"- {key.upper()}: {value}\n"
        
        # Build conversation history string
        history_str = ""
        if conversation_history:
            history_str = "\n\nPrevious Conversation:\n"
            for msg in conversation_history[-3:]:  # Last 3 messages
                history_str += f"{msg['role']}: {msg['content']}\n"
        
        # Create prompt based on whether documents were found
        if no_documents_found:
            system_message = """You are a financial analyst assistant specializing in private equity fund performance.

IMPORTANT: No relevant documents were found for this query in the database.

Your role:
- Explain that no relevant documents were found
- Suggest possible reasons (e.g., query might need rephrasing, more documents need to be uploaded)
- Provide helpful suggestions for what the user can do next
- If asked about general financial concepts, you can provide brief educational information
- Be helpful and guide the user to get better results

Do NOT:
- Hallucinate or make up specific information about documents that don't exist
- Provide specific fund data without sources"""
        else:
            system_message = """You are a financial analyst assistant specializing in private equity fund performance.

Your role:
- Answer questions about fund performance using provided context
- Calculate metrics like DPI, IRR when asked
- Explain complex financial terms in simple language
- Always cite your sources from the provided documents

When calculating:
- Use the provided metrics data
- Show your work step-by-step
- Explain any assumptions made

Format your responses:
- Be concise but thorough
- Use bullet points for lists
- Bold important numbers using **number**
- Provide context for metrics"""

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_message),
            ("user", """Context from documents:
{context}
{metrics}
{history}

Question: {query}

Please provide a helpful answer based on the context and metrics provided.""")
        ])
        
        # Generate response
        messages = prompt.format_messages(
            context=context_str,
            metrics=metrics_str,
            history=history_str,
            query=query
        )
        
        try:
            response = self.llm.invoke(messages)
            if hasattr(response, 'content'):
                return response.content
            return str(response)
        except Exception as e:
            return f"I apologize, but I encountered an error generating a response: {str(e)}"
