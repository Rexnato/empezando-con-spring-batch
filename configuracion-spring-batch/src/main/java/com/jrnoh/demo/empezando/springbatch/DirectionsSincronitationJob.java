package com.jrnoh.demo.empezando.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;


//Indica al contedor de beans de spring que la gestione
@Configuration
public class DirectionsSincronitationJob {
	
	public static final String  NAME_JOB = "directionsSincronizationJob";
	
	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	//Job que representa el batch en el contedor
	@Bean
	public Job sincronationDirection(JobRepository jobRepository,TaskletStep initStep) {
		return new JobBuilder(NAME_JOB, jobRepository)
		.start(initStep) //Tarea que realizara el job
		.build();
	}
	
	//Accion que realizara el job  , en este caso se puso que unicamente imprima un mensaje en consola 
	@Bean
	public TaskletStep initStep(JobRepository jobRepository, PlatformTransactionManager p) {
		
		return new StepBuilder("initStep", jobRepository)
				.tasklet(new Tasklet() {
					
					@Override
					public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
						logger.info("******************** Iniciando Batch ***********");
						return RepeatStatus.FINISHED;
					}
				}, p)
				.build();
	}

}

