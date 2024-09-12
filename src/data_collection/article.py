import json
from enum import Enum
from dataclasses import dataclass, field


class ArticleType(Enum):
    REGULAR = None
    WIKIPEDIA = "Wikipedia"
    TEMPLATE = "Sjabloon"
    CATEGORY = "Categorie"
    PORTAL = "Portaal"
    TALK = "Overleg sjabloon"


@dataclass
class Article:
    title: str
    exists: bool
    links: list | None = None
    type: ArticleType = field(init=False)
    num_links: int | None = field(init=False)

    def __post_init__(self):
        if self.exists:
            self.type = next(
                (
                    type
                    for type in ArticleType
                    if type.value and type.value in self.title
                ),
                ArticleType.REGULAR,
            )
            self.num_links = len(self.links) if self.links else None

    def __repr__(self) -> str:
        if self.exists:
            return f"Article(title='{self.title}', exists={self.exists}, type={self.type}, num_links={self.num_links})"
        return f"Article(title='{self.title}', exists={self.exists})"

    def __str__(self) -> str:
        return f"Article: {self.title}"

    def get_url_title(self) -> str:
        return self.title.replace(" ", "_")

    def to_json(self) -> str:
        return json.dumps(
            {
                "title": self.title,
                "exists": self.exists,
                "type": self.type.name,
                "num_links": self.num_links,
                "links": self.links,
            }
        )
