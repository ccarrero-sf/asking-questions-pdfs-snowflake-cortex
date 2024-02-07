# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Get the current credentials
session = get_active_session()

# Module vars
database='DEMO_DB'
schema='CORTEX'
docs_table = f'{database}.{schema}.UNSTRUCTURED_DOCS_WITH_CHUNKS'
docs_stage = f'{database}.{schema}.DOCS'
docs_stream = f'{database}.{schema}.DOCS_STREAM'

# Function definitions
def create_prompt (myquestion, docs_table, docs_stage, rag):
    """
    This function creates the prompt with the question we are asking. 
    If the client wants to use RAG then VECTOR_COSINE_DISTANCE() will 
    be used to find similar chunks to the question being asked and that 
    text will be added to the prompt as context. 
    
    Once the prompt has been built, the cortex function complete() 
    is called to genreate the answer to the question
    """

    prompt = f"""
         'Question:  
           {myquestion} 
           Answer: '
           """
    url_link = "None"
    relative_path = "None"

    if rag:
        myquestion_quoted = f"'{myquestion}'"
    
        cmd = f"""
        with results as
        (SELECT RELATIVE_PATH,
           VECTOR_COSINE_DISTANCE(TEXT_CHUNK_VECTOR, 
                    snowflake.ml.embed_text('e5-base-v2',{myquestion_quoted})) as distance,
           chunk
        from {docs_table}
        order by distance desc
        limit 1)
        select chunk, relative_path from results
        """
        df_context = session.sql(cmd).to_pandas()

        if not df_context.empty:
            prompt_context = df_context['CHUNK'][0]
            prompt_context = prompt_context.replace("'", "")
            relative_path =  df_context['RELATIVE_PATH'][0]
    
            prompt = f"""
             'Answer the question based on the context. Be concise
              Context: {prompt_context}
              Question:  
               {myquestion} 
               Answer: '
               """
            cmd2 = f"select GET_PRESIGNED_URL(@{docs_stage}, '{relative_path}', 360) as URL_LINK from directory(@{docs_stage})"
            df_url_link = session.sql(cmd2).to_pandas()
            
            if not df_url_link.empty:
                url_link = df_url_link['URL_LINK'][0]
            else:
                prompt = f"""
                            'Question:  
                               {myquestion} 
                               Answer: '
                               """
                url_link = "None"
                relative_path = "None"

    # else:
    #     prompt = f"""
    #      'Question:  
    #        {myquestion} 
    #        Answer: '
    #        """
    #     url_link = "None"
    #     relative_path = "None"
        
    return prompt, url_link, relative_path

def complete(myquestion, model_name, docs_table, docs_stage, rag = True):

    prompt, url_link, relative_path =create_prompt (myquestion, docs_table, docs_stage, rag)
    cmd = f"""
        select snowflake.ml.complete(
            '{model_name}',
            {prompt})
            as response
            """
    
    df_response = session.sql(cmd).collect()
    return df_response, url_link, relative_path

def display_response (question, model_name, rag):
    """
    This helper prints the response and the link to the document used if RAG has been used
    """
    response, url_link, relative_path = complete(question, model_name, docs_table, docs_stage, rag)
    st.markdown(response[0].RESPONSE)
    if rag:
        display_url = f"Link to [{relative_path}]({url_link}) that may be useful"
        st.markdown(display_url)


# Write directly to the app

st.title("PDF Insights by Snowflake Cortex :snowman:")
st.markdown("## Asking Questions to Your Own Documents")

st.markdown("""###### > You can use Cortex managed LLMs to provide answers from their own knowledge""")
st.markdown("""###### > Or you could provide them additional context using your own documents!""")
st.markdown("-----")
st.markdown(f"""###### For this demo, we're monitoring the Stage *{docs_stage}* for any additional documents""")

st.markdown(f"""###### Documents available in the Stage {docs_stage}:""")
stream_docs_available = session.sql(f"SELECT RELATIVE_PATH FROM DIRECTORY(@{docs_stage})").collect()
st.dataframe(stream_docs_available)

st.markdown(f"###### New in document stream {docs_stream}:")
docs_available = session.sql(f"SELECT RELATIVE_PATH FROM {docs_stream} WHERE METADATA$ACTION != 'DELETE'").collect()
st.dataframe(docs_available)

st.markdown(f"""###### Document processed and available in Table {docs_table}:""")
table_docs_available = session.sql(f"select distinct relative_path from {docs_table} ").collect()
st.dataframe(table_docs_available)

st.markdown("-----")
st.markdown("""###### > We'll use Cortex's COMPLETE(); it provides access to *Llama2-7b-chat* & *Llama2-70b-chat* foundational models""")
model = st.selectbox('Select your model:',('llama2-7b-chat','llama2-70b-chat'))

question = st.text_input("Enter question", placeholder="Write 3 best practices for devops", label_visibility="collapsed")

rag = st.checkbox('Use your own documents as context?')


if question:
    display_response (question, model, rag)




