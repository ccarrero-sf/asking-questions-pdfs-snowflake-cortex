import streamlit as st # Import python packages
from snowflake.snowpark.context import get_active_session
session = get_active_session() # Get the current credentials

def create_prompt (myquestion, rag):

    if rag == 1:    
        cmd = f"""
        with results as
        (SELECT RELATIVE_PATH,
           VECTOR_COSINE_DISTANCE(chunk_vec, 
                    snowflake.ml.embed_text('e5-base-v2','{myquestion}')) as distance,
           chunk
        from docs_chunks_table
        order by distance desc
        limit 1)
        select chunk, relative_path from results
        
        """
        df_context = session.sql(cmd).to_pandas()
        prompt_context = df_context._get_value(0,'CHUNK')
        prompt_context = prompt_context.replace("'", "")
        relative_path =  df_context._get_value(0,'RELATIVE_PATH')
    
        prompt = f"""
         'Answer the question based on the context. Be considse
          Context: {prompt_context}
          Question:  
           {myquestion} 
           Answer: '
           """
        cmd2 = f"select GET_PRESIGNED_URL(@docs, '{relative_path}', 360) as URL_LINK from directory(@docs)"
        df_url_link = session.sql(cmd2).to_pandas()
        url_link = df_url_link._get_value(0,'URL_LINK')

    else:
        prompt = f"""
         'Question:  
           {myquestion} 
           Answer: '
           """
        url_link = "None"
        relative_path = "None"
        
    return prompt, url_link, relative_path

def complete(myquestion, model, rag = 1):

    prompt, url_link, relative_path =create_prompt (myquestion, rag)
    cmd = f"""
        select snowflake.ml.complete(
            '{model}',
            {prompt})
            as response
            """
    
    df_response = session.sql(cmd).collect()
    return df_response, url_link, relative_path

def display_response (question, model, rag=0):
    response, url_link, relative_path = complete(question, model, rag)
    res_text = response[0].RESPONSE
    st.markdown(res_text)
    if rag == 1:
        display_url = f"Link to [{relative_path}]({url_link}) that may be useful"
        st.markdown(display_url)

st.title("Asking Questions to Your Own Documents with Snowflake Cortex:")
st.write("""You can ask questions and decide if you want to use your documents for context or allow the model to create their own response.""")
st.write("This is the list of documents you already have. Just upload new documents into your DOCS staging area and they will be automatically processed")
docs_available = session.sql("ls @docs").collect()
list_docs = []
for doc in docs_available:
    list_docs.append(doc["name"])
st.dataframe(list_docs)

st.write("Docs just added waiting to be procesed:")
stream_docs_available = session.table("docs_stream").collect()
st.dataframe(stream_docs_available)

model = st.selectbox('Select your model:',('llama2-7b-chat','llama2-70b-chat'))

question = st.text_input("Enter question", placeholder="Write 3 best practices for devops", label_visibility="collapsed")

rag = st.checkbox('Use your own documents as context?')

print (rag)

if rag:
    use_rag = 1
else:
    use_rag = 0

if question:
    display_response (question, model, use_rag)


