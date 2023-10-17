package com.jrnoh.demo.empezando.springbatch;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/***
 * 
 * @author jnoh
 *
 */

@RestController
@RequestMapping("/job")
public class JobRest {
	
	@Autowired
	JobLauncher jobLauncher;
	
	@Autowired
	Job job;
	
	@Autowired
	JobExplorer jobExplorer;
	
	/**
	 * Lanza un job 
	 * 
	 * 
	 	curl --location --request POST 'http://localhost:8080/job/ejecutar' \
		--header 'Content-Type: application/json' \
		--data-raw '{
		   "fecha" : "2023-10-16"
		}
		'
	 * 
	 * 
	 * @throws JobExecutionAlreadyRunningException
	 * @throws JobRestartException
	 * @throws JobInstanceAlreadyCompleteException
	 * @throws JobParametersInvalidException
	 * @throws ParseException 
	 */
	@PostMapping("/ejecutar")
	public ResponseEntity<HashMap<String, Object>> ejecutar(@RequestBody HashMap<String,Object> request)
	{
		var response = new HashMap<String,Object>();
		
		try {
			var fecha = new SimpleDateFormat("yyyy-MM-dd").parse(request.get("fecha").toString());
			var jobParametersBuilder = new JobParametersBuilder()
					.addDate("fecha", fecha);
			
			var jobExecution = jobLauncher.run(job, jobParametersBuilder.toJobParameters());
			
			response.put("jobId", jobExecution.getId());
			
		} catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException
				| JobParametersInvalidException e ) 
		{
			response.put("errorMessage :", e.getMessage());
			
		}catch (Exception e) {
			response.put("errorMessage :", "Ocurrio un error no controlado");
		}
		
		return ResponseEntity.ok(response);
		
	}
	
	
	/***
	 curl --location 'http://localhost:8080/job/job-execution/poneridaca' \
	 * @return
	 */
	@GetMapping("/job-execution/{jobExecutionId}")
	public ResponseEntity<String> obtenerJobsInstances(@PathVariable Long jobExecutionId){
		
		var jobExecution =  jobExplorer.getJobExecution(jobExecutionId);
		
		return ResponseEntity.ok(jobExecution == null ? "" : jobExecution.toString());
	}

}
