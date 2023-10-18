package com.jrnoh.demo.empezando.springbatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.data.builder.RepositoryItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;


//Indica al contedor de beans de spring que la gestione
@Configuration
@EnableBatchProcessing()
public class DirectionsSincronitationJob {
	
	public static final String  NAME_JOB = "directionsSincronizationJob";
	
	Logger logger = LoggerFactory.getLogger(this.getClass());
	
	//Job que representa el batch en el contedor
	@Bean
	public Job sincronationDirection(JobRepository jobRepository,TaskletStep initStep,Step importDirections) {
		return new JobBuilder(NAME_JOB, jobRepository)
		.start(initStep) //Tarea que realizara el job
		.next(importDirections)
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
	
	/***
	 * Paso generado para la importacion de las direcciones
	 * @param jobRepository
	 * @param p
	 * @param fileReaderDirections
	 * @param procesorDirections
	 * @param writerDirections
	 * @return
	 */
	@Bean
	public Step importDirections(JobRepository jobRepository,PlatformTransactionManager p
			,FlatFileItemReader<DirectionRecord> fileReaderDirections
			,ItemProcessor<DirectionRecord, Direction> procesorDirections
			, RepositoryItemWriter<Direction> writerDirections) {
		
		return new StepBuilder("importDirections",jobRepository)
		.<DirectionRecord,Direction>chunk(10, p)//fragmentos de 10 
		.reader(fileReaderDirections)
		.processor(procesorDirections)
		.writer(writerDirections)
		.build();
	}
	
	/***
	 * Lectura del archivo
	 * @return
	 */
	@Bean
	public FlatFileItemReader<DirectionRecord> fileReaderDirections(){
		
		return new FlatFileItemReaderBuilder<DirectionRecord>()
				.name("fileReaderDirections")
				.resource(new FileSystemResource("staging/directions.csv")) // ubicacion del archivo
				.delimited()
				.names("codigoPostal","asentamiento","tipoAsentamiento","municipio")
				.targetType(DirectionRecord.class) // clase en la que se vacia el cisv
				.build();
	}
	
	@Bean
	public ItemProcessor<DirectionRecord, Direction> procesorDirections(){
		
		//convierte el direction record a una direction (entidad)
		return new ItemProcessor<DirectionRecord, Direction>() {

			@Override
			public Direction process(DirectionRecord d) throws Exception {
				Direction direction = new Direction();
				direction.setAsentamiento(d.asentamiento());
				direction.setCodigoPostal(d.codigoPostal());
				direction.setMunicipio(d.municipio());
				direction.setTipoAsentamiento(d.tipoAsentamiento());
				
				return direction;
			}
				
		};
	}
	
	/***
	 * Se encarga de persistir por entidades 
	 * @param directionRepository
	 * @return
	 */
	@Bean
	public RepositoryItemWriter<Direction> writerDirections(DirectionRepository directionRepository){
		
		return new RepositoryItemWriterBuilder<Direction>()
		.repository(directionRepository)
		.build();
	}

}

