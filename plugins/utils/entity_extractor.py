# plugins/utils/entity_extractor.py
import logging
import json
import re
from typing import Dict, Any, List

class EntityExtractor:
    """
    Utility class for extracting entities and relationships from documents.
    """
    
    def __init__(self, llm):
        """
        Initialize with a LangChain LLM.
        
        :param llm: LangChain LLM instance
        """
        self.llm = llm
        self.logger = logging.getLogger(__name__)
    
    def extract_entities(self, document) -> Dict[str, Any]:
        """
        Extract entities and relationships from document summary.
        
        Args:
            document: Document to extract entities from
            
        Returns:
            Dict with entities and relationships
        """
        # Use the document summary instead of full content
        if not document.summary:
            self.logger.warning(f"Document {document.url} has no summary for entity extraction")
            return {"entities": [], "relationships": []}
        
        prompt = f"""Extract entities and their relationships from this government document summary.
For each entity, provide both the exact mention in the text AND a canonical (standardized) name.

Title: {document.title}
Source: {document.source_name} - {document.subsource_name}

Summary:
{document.summary}

IMPORTANT GUIDELINES:
1. Focus ONLY on meaningful entities like Agencies, People, Organizations, Laws, Programs, Assets, Resources, and Policies.
2. DO NOT include years, dates, percentages, or numeric values as entities.
3. DO NOT include general concepts, statistics or measurements as entities.
4. Each entity should represent a specific named organization, person, program, or policy.

Format your response as a structured JSON:

```json
{{
"entities": [
    {{
    "mention": "The exact text as it appears",
    "canonical_name": "The standardized name",
    "entity_type": "Person/Agency/Policy/Program/Law/Organization/etc."
    }}
],
"relationships": [
    {{
    "source_mention": "The source entity mention",
    "source_canonical": "The source entity canonical name",
    "relation": "OVERSEES/IMPLEMENTS/PART_OF/LOCATED_IN/etc.",
    "target_mention": "The target entity mention",
    "target_canonical": "The target entity canonical name"
    }}
]
}}
EXAMPLES OF VALID ENTITIES:
✓ "Federal Reserve" (Agency)
✓ "Jerome Powell" (Person)
✓ "Clean Water Act" (Law)
✓ "Paycheck Protection Program" (Program)
EXAMPLES OF INVALID ENTITIES (DO NOT INCLUDE THESE):
✗ "2025" (year)
✗ "8.9%" (percentage)
✗ "Q1" (time period)
✗ "inflation" (concept)
✗ "$200 million" (monetary value)
"""
        try:
            response = self.llm.invoke(prompt)
            response_text = response.content.strip()
            
            # Extract JSON part
            json_match = re.search(r'```json\s*(.*?)\s*```', response_text, re.DOTALL)
            
            if json_match:
                json_str = json_match.group(1)
            else:
                # Try to find JSON without the markdown code block
                json_match = re.search(r'({[\s\S]*})', response_text)
                if json_match:
                    json_str = json_match.group(1)
                else:
                    self.logger.warning("Could not extract JSON from LLM response")
                    return {"entities": [], "relationships": []}
            
            # Clean up the JSON string - fix common issues
            json_str = json_str.strip()
            
            # Fix trailing commas (common JSON error)
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            
            # Try to parse JSON
            try:
                extraction_result = json.loads(json_str)
            except json.JSONDecodeError as e:
                self.logger.warning(f"JSON parse error: {e}. Attempting to recover...")
                
                # Create basic structure
                extraction_result = {"entities": [], "relationships": []}
                
                # Try to extract entities with regex
                entity_matches = re.findall(r'"mention":\s*"([^"]+)"[^}]+?"canonical_name":\s*"([^"]+)"[^}]+?"entity_type":\s*"([^"]+)"', json_str)
                for mention, canonical, entity_type in entity_matches:
                    extraction_result["entities"].append({
                        "mention": mention,
                        "canonical_name": canonical,
                        "entity_type": entity_type
                    })
                
                # Try to extract relationships with regex
                rel_matches = re.findall(r'"source_canonical":\s*"([^"]+)"[^}]+?"relation":\s*"([^"]+)"[^}]+?"target_canonical":\s*"([^"]+)"', json_str)
                for source, relation, target in rel_matches:
                    extraction_result["relationships"].append({
                        "source_canonical": source,
                        "relation": relation,
                        "target_canonical": target,
                        "source_mention": source,  # Fallback
                        "target_mention": target   # Fallback
                    })
            
            # Filter out invalid entities
            filtered_entities = []
            for entity in extraction_result.get("entities", []):
                canonical_name = entity.get("canonical_name", "")
                
                # Skip if entity is numeric, a year, or a percentage
                if (re.match(r'^[0-9]+(\.[0-9]+)?%?$', canonical_name) or 
                    re.match(r'^(19|20)\d{2}$', canonical_name) or  # Years like 1999, 2025
                    re.match(r'^Q[1-4]$', canonical_name) or        # Quarters like Q1, Q2
                    canonical_name.lower() in ["inflation", "recession", "recovery", "growth"] or  # Generic concepts
                    len(canonical_name) < 3):  # Very short strings
                    continue
                    
                filtered_entities.append(entity)
            
            # Update entities with filtered list
            extraction_result["entities"] = filtered_entities
            
            # Filter relationships that reference filtered entities
            valid_entities = {entity["canonical_name"] for entity in filtered_entities}
            filtered_relationships = []
            
            for rel in extraction_result.get("relationships", []):
                source = rel.get("source_canonical", "")
                target = rel.get("target_canonical", "")
                
                if source in valid_entities and target in valid_entities:
                    filtered_relationships.append(rel)
            
            # Update relationships with filtered list
            extraction_result["relationships"] = filtered_relationships
            
            self.logger.info(f"Extracted {len(extraction_result.get('entities', []))} entities and {len(extraction_result.get('relationships', []))} relationships from document summary")
            
            return extraction_result
        
        except Exception as e:
            self.logger.error(f"Error extracting entities: {e}")
            return {"entities": [], "relationships": []}