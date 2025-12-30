#Using python to set up all my local environment stuff to the 3.9-slim image, a blank image Ill copy my stuff too
FROM python:3.9-slim

#Sets the directory /app in my container, not local code editor
WORKDIR /app

#Makes my logs appear instead of happen silently
ENV PYTHONUNBUFFERED=1

#Copy my requirements.txt file in my local environment to the current directory (/app)
COPY requirements.txt .

#Run and read requirments.txt with no extra cache fluff
RUN pip install --no-cache-dir -r requirements.txt

#Create a scripts directory inside the container with my scripts copied in the same structure
COPY scripts/ ./scripts/

#Copy run_pipeline.sh to current directory
COPY run_pipeline.sh .

#On waking up, run the run_pipeline.sh file
CMD ["bash", "run_pipeline.sh"]