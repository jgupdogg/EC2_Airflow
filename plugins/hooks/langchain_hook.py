from airflow.hooks.base import BaseHook
from langchain.schema import Document as LCDocument
import logging
import os


class LangChainHook(BaseHook):
    """
    Hook for interacting with LangChain components.
    """
    
    def __init__(self, anthropic_conn_id='anthropic_default', openai_conn_id='openai_default'):
        super().__init__()
        self.anthropic_conn_id = anthropic_conn_id
        self.openai_conn_id = openai_conn_id
        self._llm = None
        self._embedding_model = None
        self.logger = logging.getLogger(__name__)
    
    def get_llm(self, model_name="claude-3-haiku-20240307", temperature=0.3):
        """Get a LangChain LLM."""
        if self._llm is not None:
            return self._llm
            
        conn = self.get_connection(self.anthropic_conn_id)
        anthropic_api_key = conn.password
        
        if not anthropic_api_key:
            raise ValueError("Anthropic API key is required")
        
        try:
            from langchain_anthropic import ChatAnthropic
            
            self._llm = ChatAnthropic(
                model=model_name,
                anthropic_api_key=anthropic_api_key,
                temperature=temperature
            )
            self.logger.info(f"Initialized Anthropic Claude model: {model_name}")
            return self._llm
        except Exception as e:
            self.logger.error(f"Error initializing LLM: {e}")
            raise
    
    def get_embedding_model(self, model_name="text-embedding-3-small"):
        """Get a LangChain embedding model."""
        if self._embedding_model is not None:
            return self._embedding_model
            
        conn = self.get_connection(self.openai_conn_id)
        openai_api_key = conn.password
        
        if not openai_api_key:
            raise ValueError("OpenAI API key is required")
        
        try:
            from langchain_openai import OpenAIEmbeddings
            
            self._embedding_model = OpenAIEmbeddings(
                model=model_name,
                openai_api_key=openai_api_key
            )
            self.logger.info(f"Initialized OpenAI embedding model: {model_name}")
            return self._embedding_model
        except Exception as e:
            self.logger.error(f"Error initializing embedding model: {e}")
            raise
    
    def generate_summary(self, document, max_content_length=8000):
        """Generate a summary for a document."""
        if not document.content:
            raise ValueError("Document has no content to summarize")
        
        llm = self.get_llm()
        
        prompt = f"""Generate a structured summary of the following government website content.

Input:
Title: {document.title}
Source: {document.source_name} - {document.subsource_name}
URL: {document.url}

Content:
{document.content[:max_content_length]}

Output Format:
1. TITLE: A clear, direct title that captures the main topic (no more than 10 words)
2. FACTS: 3-5 bullet points with the most important and relevant information 
3. SENTIMENT: One bullet point expressing the overall sentiment (positive, negative, or neutral) of the content
4. TAGS: 5-7 relevant keywords/tags separated by commas
"""

        response = llm.invoke(prompt)
        return response.content.strip()
    
    def test_connection(self):
        """Test connections to LLM and embedding models."""
        try:
            # Test LLM
            llm = self.get_llm()
            response = llm.invoke("Say hello!")
            llm_success = "hello" in response.content.lower()
            
            # Test embedding model
            embedding_model = self.get_embedding_model()
            embed = embedding_model.embed_query("Test")
            embed_success = len(embed) > 0
            
            if llm_success and embed_success:
                return True, "LLM and embedding model connections successful"
            else:
                return False, "Failed to verify LLM or embedding model"
        except Exception as e:
            return False, str(e)