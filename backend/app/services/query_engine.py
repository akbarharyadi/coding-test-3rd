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
from app.services.search_service import SearchService
from app.services.metrics_calculator import MetricsCalculator
from app.services.cache_service import cache_service
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class QueryEngine:
    """RAG-based query engine for fund analysis"""

    def __init__(self, db: Session, use_cache: bool = True):
        self.db = db
        self.search_service = SearchService(db=db)
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

        # Extract fund context from conversation history if fund_id not provided
        if not fund_id and conversation_history:
            fund_id = await self._extract_fund_from_history(conversation_history, query)
            if fund_id:
                logger.info(f"Extracted fund_id {fund_id} from conversation history")

        # Step 1: Classify query intent
        intent = await self._classify_intent(query)

        # Step 2: Retrieve relevant context using search service (supports FAISS and PostgreSQL)
        relevant_docs = await self.search_service.search(
            query=query,
            k=settings.TOP_K_RESULTS,
            fund_id=fund_id,
            include_content=True
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
            no_documents_found=no_documents_found,
            intent=intent
        )

        processing_time = time.time() - start_time

        result = {
            "answer": answer,
            "sources": [
                {
                    "content": doc.get("content", ""),
                    "metadata": doc.get("metadata", {}),
                    "score": doc.get("score")
                }
                for doc in relevant_docs
                if doc.get("content")  # Only include sources with content
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

        # Definition keywords - CHECK FIRST to catch "what does X mean" before other patterns
        def_keywords = [
            "what does", "what is", "mean", "define", "explain", "definition",
            "what are", "tell me about", "describe"
        ]
        if any(keyword in query_lower for keyword in def_keywords):
            # But exclude if it's specifically asking for current/specific fund metrics
            if not any(specific in query_lower for specific in ["what is the current", "what is the dpi for", "what is the irr for"]):
                return "definition"

        # Calculation keywords - more specific now
        calc_keywords = [
            "calculate", "what is the current", "what is the dpi for",
            "what is the irr for", "what is the tvpi for", "compute"
        ]
        if any(keyword in query_lower for keyword in calc_keywords):
            return "calculation"

        # Retrieval keywords
        ret_keywords = [
            "show me", "list", "all", "find", "search", "when",
            "how many", "which", "largest", "smallest", "most recent"
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
        no_documents_found: bool = False,
        intent: str = "general"
    ) -> str:
        """Generate response using LLM"""

        # Build context string
        if no_documents_found:
            context_str = "[No relevant documents found in the database]"
        else:
            context_str = "\n\n".join([
                f"[Source {i+1}]\n{doc.get('content', '')}"
                for i, doc in enumerate(context[:3])  # Use top 3 sources
                if doc.get('content')  # Only include docs with content
            ])

        # Build metrics string
        metrics_str = ""
        if metrics:
            metrics_str = "\n\nAvailable Metrics:\n"
            for key, value in metrics.items():
                if value is not None:
                    metrics_str += f"- {key.upper()}: {value}\n"

        # Build conversation history string - Include ALL messages for better context
        history_str = ""
        has_history = conversation_history and len(conversation_history) > 0
        if has_history:
            history_str = "\n\nPrevious Conversation:\n"
            for msg in conversation_history:  # Include all messages
                role = msg.get('role', 'user')
                content = msg.get('content', '')
                history_str += f"{role}: {content}\n"
        
        # Create prompt based on whether documents were found and conversation history
        if no_documents_found:
            if has_history:
                system_message = """You are a financial analyst assistant specializing in private equity fund performance.

IMPORTANT: No relevant documents were found for this query in the database.

CRITICAL - Conversation Context:
- Review the Previous Conversation section carefully
- If a fund was mentioned previously, remember that context
- When the user refers to "this fund" or "the fund", they mean the fund discussed earlier in the conversation
- Maintain continuity across the conversation

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

IMPORTANT: No relevant documents were found for this query in the database.

Your role:
- Explain that no relevant documents were found
- Suggest that the user specify which fund they're asking about
- Ask clarifying questions if the query is ambiguous
- Provide helpful suggestions for what the user can do next
- If asked about general financial concepts, you can provide brief educational information
- Be helpful and guide the user to get better results

Do NOT:
- Hallucinate or make up specific information about documents that don't exist
- Provide specific fund data without sources
- Assume which fund the user is asking about - ask for clarification"""
        else:
            if has_history:
                system_message = """You are a financial analyst assistant specializing in private equity fund performance.

CRITICAL - Conversation Context:
- ALWAYS review the "Previous Conversation" section first before answering
- If a fund was mentioned in previous messages, remember that fund's name and context
- When the user asks about "this fund", "the fund", or uses "it", they are referring to the fund discussed earlier
- Maintain conversation continuity - if metrics were calculated for a fund previously, remember that fund
- Pay special attention to fund names mentioned by either user or assistant in the conversation history

Your role:
- Answer questions about fund performance using provided context
- Calculate metrics like DPI, IRR when asked
- Explain complex financial terms in simple language
- Always cite your sources from the provided documents
- Reference the fund name when answering to confirm you understand the context

When calculating:
- Use the provided metrics data
- Show your work step-by-step
- Explain any assumptions made
- State which fund the calculation is for

Format your responses:
- Be concise but thorough
- Use bullet points for lists
- Bold important numbers using **number**
- Provide context for metrics
- Start answers by acknowledging the fund being discussed if it was mentioned before"""
            else:
                # Check if this is a definition query
                if intent == "definition":
                    system_message = """You are a financial analyst assistant specializing in private equity fund performance.

Your role:
- Explain financial concepts, terms, and metrics clearly and concisely
- Provide educational information about private equity and fund performance
- Use simple language to explain complex financial terms
- You may use examples from the provided documents to illustrate concepts, but this is optional
- Focus on explaining the concept itself, not on calculating specific values

IMPORTANT for general/educational questions:
- The user is asking for an explanation or definition, not specific fund data
- You do NOT need to calculate anything or reference specific funds
- You do NOT need to identify which fund the sources relate to
- Simply explain the concept clearly

Format your responses:
- Be concise but thorough
- Use bullet points for clarity
- Provide formulas when relevant (e.g., "DPI = Total Distributions / Paid-In Capital")
- Give context about when and why the metric is used
- You may optionally provide examples if helpful"""
                else:
                    system_message = """You are a financial analyst assistant specializing in private equity fund performance.

Your role:
- Answer questions about fund performance using provided context
- Calculate metrics like DPI, IRR when asked
- Explain complex financial terms in simple language
- Always cite your sources from the provided documents
- If the document sources mention a specific fund name, reference it in your answer
- If multiple funds are found in the sources, clarify which fund you're discussing

IMPORTANT for first questions:
- The user has not mentioned any fund previously in this conversation
- Base your answer ONLY on the provided document context
- Identify which fund(s) the documents relate to from the source metadata
- If sources are from multiple funds, clarify this to the user

When calculating:
- Use the provided metrics data
- Show your work step-by-step
- Explain any assumptions made
- State which fund the calculation is for based on the source documents

Format your responses:
- Be concise but thorough
- Use bullet points for lists
- Bold important numbers using **number**
- Provide context for metrics
- Always identify the fund name from the sources when answering"""

        # Build user prompt based on whether there's conversation history
        if has_history:
            user_prompt = """{history}

Context from documents:
{context}
{metrics}

Current Question: {query}

Please provide a helpful answer. Remember to consider the previous conversation when interpreting this question."""
        else:
            user_prompt = """Context from documents:
{context}
{metrics}

Question: {query}

Please provide a helpful answer based on the provided context."""

        prompt = ChatPromptTemplate.from_messages([
            ("system", system_message),
            ("user", user_prompt)
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

    async def _extract_fund_from_history(
        self,
        conversation_history: List[Dict[str, str]],
        current_query: str
    ) -> Optional[int]:
        """
        Extract fund ID from conversation history by matching fund names

        Args:
            conversation_history: Previous conversation messages
            current_query: Current user query

        Returns:
            fund_id if a fund is found, None otherwise
        """
        try:
            from app.models.fund import Fund
            from sqlalchemy import func

            # Combine conversation history and current query
            full_context = "\n".join([
                f"{msg.get('role', 'user')}: {msg.get('content', '')}"
                for msg in conversation_history[-5:]  # Last 5 messages for context
            ])
            full_context += f"\nuser: {current_query}"

            # Get all fund names from database
            funds = self.db.query(Fund.id, Fund.name, Fund.gp_name).all()

            # Check if any fund name appears in the conversation context
            # Case-insensitive matching
            context_lower = full_context.lower()

            for fund in funds:
                # Check fund name
                if fund.name and fund.name.lower() in context_lower:
                    logger.info(f"Found fund '{fund.name}' in conversation history")
                    return fund.id

                # Also check GP name for additional matching
                if fund.gp_name and fund.gp_name.lower() in context_lower:
                    # Verify this is mentioned with fund-related keywords
                    if any(keyword in context_lower for keyword in [
                        'fund', 'performance', 'dpi', 'irr', 'tvpi', 'capital',
                        'distribution', 'portfolio', 'investment'
                    ]):
                        logger.info(f"Found fund by GP name '{fund.gp_name}' in conversation history")
                        return fund.id

            return None

        except Exception as e:
            logger.error(f"Error extracting fund from history: {e}")
            return None
