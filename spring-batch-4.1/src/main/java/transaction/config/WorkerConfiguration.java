package transaction.config;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.ClassPathResource;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import transaction.domain.Transaction;
import transaction.domain.TransactionProcessor;
import transaction.domain.TransactionWriter;

@Configuration
@EnableBatchProcessing
@EnableBatchIntegration
@Profile("worker")
public class WorkerConfiguration {

	private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;


	public WorkerConfiguration(RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory) {
		this.workerStepBuilderFactory = workerStepBuilderFactory;
	}

	@Bean
	public DirectChannel requests() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {
		return IntegrationFlows
				.from(Amqp.inboundAdapter(connectionFactory, "requests"))
				.channel(requests())
				.get();
	}

	@Bean
	public DirectChannel replies() {
		return new DirectChannel();
	}

	@Bean
	public IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
		return IntegrationFlows.from(replies())
				.handle(Amqp.outboundAdapter(amqpTemplate).routingKey("replies"))
				.get();
	}

	@Bean
	public Step workerStep() {
		return this.workerStepBuilderFactory.get("workerStep")
				.inputChannel(requests())
				.outputChannel(replies())
				.<Transaction, Transaction>chunk(3)
				.reader(reader(null))
				.processor(processor())
				.writer(writer(null))
				.build();
	}

	@Bean
	@StepScope
	public FlatFileItemReader<Transaction> reader(@Value("#{stepExecutionContext[fileName]}") String fileName) {
		FlatFileItemReader<Transaction> reader = new FlatFileItemReader<Transaction>();
		reader.setResource(new ClassPathResource(fileName));
		reader.setLineMapper(new DefaultLineMapper<Transaction>() {{
			setLineTokenizer(new DelimitedLineTokenizer() {{
				setNames(new String[]{"transactionId", "merchantId", "transactionAmt"});
			}});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<Transaction>() {{
				setTargetType(Transaction.class);
			}});
		}});
		return reader;
	}

	@Bean
	@StepScope
	public TransactionProcessor processor() {
		return new TransactionProcessor();
	}

	@Bean
	@StepScope
	public TransactionWriter writer(@Value("#{stepExecutionContext[outputFile]}") String outputFile) {
		TransactionWriter writer = new TransactionWriter();
		writer.setOutputFile(outputFile);
		return writer;
	}
}
