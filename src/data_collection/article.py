"""Article data model for Wikipedia network analysis.

This module defines the Article class and ArticleType enum for representing
and categorizing Wikipedia articles in the network analysis system.
"""

import json
from dataclasses import dataclass, field
from enum import Enum


class ArticleType(Enum):
    """Enumeration of different types of Wikipedia articles."""

    REGULAR = None
    WIKIPEDIA = "Wikipedia"
    TEMPLATE = "Sjabloon"
    CATEGORY = "Categorie"
    PORTAL = "Portaal"
    TALK = "Overleg sjabloon"


@dataclass
class Article:
    """Represents a Wikipedia article with metadata and links.

    This class stores information about a Wikipedia article including
    its title, existence status, links to other articles, and article type.
    """

    title: str
    exists: bool
    links: list | None = None
    type: ArticleType = field(init=False)
    num_links: int | None = field(init=False)

    def __post_init__(self):
        """Initialize computed fields after object creation."""
        if self.exists:
            self.type = next(
                (type for type in ArticleType if type.value and type.value in self.title),
                ArticleType.REGULAR,
            )
            self.num_links = len(self.links) if self.links else None

    def __repr__(self) -> str:
        """Return a detailed string representation of the Article."""
        if self.exists:
            return f"Article(title='{self.title}', exists={self.exists}, type={self.type}, num_links={self.num_links})"
        return f"Article(title='{self.title}', exists={self.exists})"

    def __str__(self) -> str:
        """Return a simple string representation of the Article."""
        return f"Article: {self.title}"

    def get_url_title(self) -> str:
        """Convert article title to URL-safe format by replacing spaces with underscores."""
        return self.title.replace(" ", "_")

    def to_json(self) -> str:
        """Serialize the Article to JSON format for storage."""
        return json.dumps(
            {
                "title": self.title,
                "exists": self.exists,
                "type": self.type.name,
                "num_links": self.num_links,
                "links": self.links,
            }
        )
