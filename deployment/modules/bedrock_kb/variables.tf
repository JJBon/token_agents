# modules/bedrock_kb_pgvector/variables.tf
variable "name"                  { type = string }
variable "embedding_model_arn" {
  type    = string
  default = "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0"
  description = "Amazon Titan Text Embeddings V2 (default 1024-dim) on Bedrock."
}
# e.g., arn:aws:bedrock:...:model/amazon.titan-embed-text-v2:0
variable "aurora_cluster_arn"    { type = string }
variable "rds_secret_arn"        { type = string }
variable "database_name"         { type = string }
variable "table_name"            { type = string }

# field mapping
variable "pk_field"              { type = string } # e.g., "id"
variable "text_field"            { type = string } # e.g., "chunks"
variable "vector_field"          { type = string } # e.g., "embedding"
variable "metadata_field"        { type = string } # e.g., "metadata"
variable "custom_metadata_field" { 
  type = string  
  default = "custom_metadata" 
  }

# optional S3 data source for this KB
variable "source_bucket_arn"     { type = string }
variable "source_prefixes"       { 
  type = list(string) 
  default = [] 
  }  # e.g., ["research/", "pdfs/"]

# Chunking config (fixed size as a sane default)
variable "chunk_max_tokens"      {
   type = number 
   default = 800 
   }
variable "chunk_overlap_pct"     { 
  type = number 
  default = 10 
  }