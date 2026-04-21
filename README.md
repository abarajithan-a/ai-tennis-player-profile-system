## AI based tennis player profile system
Repository for AI generated tennis player profile system  

**System Requirements & Pre-requisites & Setup:**    
1. Machine with atleast 16GB RAM
2. Python version 3.12.x
3. Postgres Database installed, up and running
4. Gemini API Key configured at https://aistudio.google.com/
5. Groq API Key configured at https://console.groq.com/
6. Install all dependent python libraries using the command,  
	`pip install requirements.txt` 
7. Download llama 8B model using the command,  
	`ollama pull llama3.1:8b`  
8. Ollama local service up and running using the command,  
	`ollama serve`

**Data Architecture:**    
![](images/ai_tennis_player_profile_system.jpg)   

**Database Schema Design:**    
![](images/tennis_postgresdb_design.jpg)   

**Run the script:**    
Go to the folder "ai_tennis_player_profile_system" and run the command  
```
python master.py
```
