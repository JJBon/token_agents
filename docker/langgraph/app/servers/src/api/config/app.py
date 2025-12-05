from pydantic_settings import BaseSettings


class DocumentationSettings(BaseSettings):
    """
    OpenAPI documentation settings
    """

    description: str = """A suite of APIs for building and integrating chatbot features."""
    title: str = "DataFrame Chatbot"
    version: str = "0.1.0"
