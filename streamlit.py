import os
from dotenv import load_dotenv
import re
import ast
import pandas as pd
import streamlit as st
from typing import List, Dict, Tuple, Optional

load_dotenv()

# ---- LangChain + FAISS (local embeddings) ----
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain.schema import Document

# ---- Perplexity chat ----
from langchain_community.chat_models import ChatPerplexity
from langchain.schema import HumanMessage, SystemMessage

# ----------------------------
# Config
# ----------------------------
CSV_PATH = "movie_full_documents_new.csv"
INDEX_DIR = "faiss_index_movies"
EMB_MODEL = "sentence-transformers/all-MiniLM-L6-v2"

HEX24 = re.compile(r"\b[0-9a-f]{24}\b")

SYSTEM_PROMPT = """  
You are a strict and precise movie knowledge assistant.  

You have access to movie metadata including:  
- movie id, title, year, plot, full plot, genres, runtime  
- cast, directors, writers  
- ratings (IMDB, Metacritic)  
- awards (text, wins, nominations)  
- embedded_movies (_id, plot_embedding)  
- comments/reviews (user, text)  
- theaters (location, geo)  

Rules:  
1. Only answer from the provided dataset context.  
2. If the answer is not present in dataset, reply exactly: "Not found in dataset".  
3. Do not hallucinate or invent any information.  
4. Keep answers short and factual unless asked for "full details".  
5. Always return lists in plain text bullet points (not JSON).  

Supported question types:  
- list the movies released in year <year>  
- list movies directed by <director>  
- explain the full details of the movie <movie>  
- list movies with runtime lower than <min>  
- group movies by year and rating  
- who is the director of <movie>  
- what are the cast and writers of <movie>  
- what is the plot of <movie>  
"""

# ----------------------------
# Helpers
# ----------------------------
def load_documents(csv_path: str) -> Tuple[List[Document], Dict[str, Document]]:
    df = pd.read_csv(csv_path)
    docs: List[Document] = []
    by_id: Dict[str, Document] = {}

    def extract_title(text: str) -> str:
        m = re.search(r"^Title:\s*(.+)$", text, flags=re.MULTILINE)
        return m.group(1).strip() if m else ""

    for _, row in df.iterrows():
        movie_id = str(row["movie_id"])
        content = str(row["document"])
        title = extract_title(content)
        metadata = {"movie_id": movie_id, "title": title}
        doc = Document(page_content=content, metadata=metadata)
        docs.append(doc)
        by_id[movie_id] = doc
    return docs, by_id


def get_or_build_index(docs: List[Document]) -> Tuple[FAISS, HuggingFaceEmbeddings]:
    embeddings = HuggingFaceEmbeddings(model_name=EMB_MODEL)
    if os.path.isdir(INDEX_DIR):
        vs = FAISS.load_local(INDEX_DIR, embeddings, allow_dangerous_deserialization=True)
    else:
        vs = FAISS.from_documents(docs, embeddings)
        vs.save_local(INDEX_DIR)
    return vs, embeddings


def parse_field_from_doc(doc_text: str, field_name: str) -> Optional[str]:
    pat = rf"^{field_name}:\s*(.+)$"
    m = re.search(pat, doc_text, flags=re.MULTILINE)
    return m.group(1).strip() if m else None


def parse_list_field(doc_text: str, field_name: str) -> List[str]:
    raw = parse_field_from_doc(doc_text, field_name)
    if not raw:
        return []
    try:
        val = ast.literal_eval(raw)
        if isinstance(val, list):
            return [str(x) for x in val]
    except Exception:
        pass
    return [x.strip().strip("'\"") for x in raw.split(",") if x.strip()]


def find_id_in_query(q: str) -> Optional[str]:
    m = HEX24.search(q.lower())
    return m.group(0) if m else None


def count_movies(df_csv_path: str) -> int:
    df = pd.read_csv(df_csv_path, usecols=["movie_id"])
    return df["movie_id"].nunique()


def build_llm() -> ChatPerplexity:
    api_key = os.environ.get("PPLX_API_KEY")
    if not api_key:
        raise RuntimeError("Set your Perplexity API key via env PPLX_API_KEY or hardcode.")
    return ChatPerplexity(
        model="sonar", #"sonar reasoning", "sonar deep research"
        temperature=0.0,
        #model_kwargs={"disable_search": True},
        pplx_api_key=api_key
    )


def build_context(docs: List[Document], max_chars: int = 12000) -> str:
    parts, total = [], 0
    for d in docs:
        head = f"[SOURCE movie_id={d.metadata.get('movie_id','?')} title={d.metadata.get('title','?')}]\n"
        chunk = head + d.page_content.strip()
        if total + len(chunk) > max_chars:
            break
        parts.append(chunk)
        total += len(chunk)
    return "\n\n---\n\n".join(parts)


def answer_question(query: str, vs: FAISS, id_lookup: Dict[str, Document], df_csv_path: str, llm: ChatPerplexity) -> str:
    q_lower = query.lower().strip()

    # Deterministic
    if re.search(r"\b(how many|total).*(movies|movie)\b", q_lower):
        return f"There are {count_movies(df_csv_path)} movies in the dataset."

    movie_id = find_id_in_query(q_lower)
    if movie_id and movie_id in id_lookup:
        doc = id_lookup[movie_id]
        if "cast" in q_lower:
            cast_list = parse_list_field(doc.page_content, "Cast")
            cast_txt = ", ".join(cast_list) if cast_list else "Not available"
            title = doc.metadata.get("title", "")
            return f"movie_id: {movie_id}\nTitle: {title}\nCast: {cast_txt}"
        if "plot" in q_lower or "summary" in q_lower:
            title = doc.metadata.get("title", "")
            plot = parse_field_from_doc(doc.page_content, "Plot") or "Not available"
            return f"movie_id: {movie_id}\nTitle: {title}\nPlot: {plot}"
        context = build_context([doc])
        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=f"CONTEXT:\n{context}\n\nQuestion: {query}")
        ]
        resp = llm.invoke(messages)
        return resp.content.strip()

    retriever = vs.as_retriever(search_type="similarity", search_kwargs={"k": 5, "fetch_k": 20})
    retrieved = retriever.get_relevant_documents(query)
    if not retrieved:
        return "I don't know from the data."
    context = build_context(retrieved)
    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=f"CONTEXT:\n{context}\n\nQuestion: {query}")
    ]
    resp = llm.invoke(messages)
    answer = resp.content.strip()
    return answer if answer else "I don't know from the data."


# ----------------------------
# Streamlit App
# ----------------------------
def main():
    st.title("🎬 Movie Q&A Assistant (RAG + Perplexity)")

    # Load docs + FAISS
    with st.spinner("Loading data and building index..."):
        docs, by_id = load_documents(CSV_PATH)
        vs, _ = get_or_build_index(docs)
        llm = build_llm()

    #st.success(f"Loaded {len(docs)} movie documents.")

    st.sidebar.header("Quick Questions")

    default_qs = [
        "How many movies are there?",
        "List movies directed by Jing Wong",
        "Who acted in movie id {}".format(docs[0].metadata["movie_id"] if docs else ""),
        "What is the plot of movie id {}".format(docs[0].metadata["movie_id"] if docs else "")
    ]

    # Initialize state
    if "user_q" not in st.session_state:
        st.session_state.user_q = ""
    if "auto_submit" not in st.session_state:
        st.session_state.auto_submit = False

    # Sidebar quick questions
    for q in default_qs:
        if st.sidebar.button(q):
            st.session_state.user_q = q
            st.session_state.auto_submit = True   # trigger auto-submit

    # Input box tied to session state
    user_q = st.text_input("Ask your own question:", key="user_q")

    # Manual submit button
    submit_clicked = st.button("Submit")

    # Either manual submit OR auto-submit from quick question
    if (submit_clicked or st.session_state.auto_submit) and user_q:
        with st.spinner("Thinking..."):
            ans = answer_question(user_q, vs, by_id, CSV_PATH, llm)
        st.markdown(f"**Q:** {user_q}\n\n**A:** {ans}")
        st.session_state.auto_submit = False  # reset flag

if __name__ == "__main__":
    main()
