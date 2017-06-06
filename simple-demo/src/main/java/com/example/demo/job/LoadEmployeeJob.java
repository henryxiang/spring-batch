package com.example.demo.job;

import com.example.demo.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@EnableBatchProcessing
public class LoadEmployeeJob {
    private static final Logger logger = LoggerFactory.getLogger(LoadEmployeeJob.class);

    private static final int CHUNK_SIZE = 500;

    @Autowired
    JobBuilderFactory jobBuilderFactory;
    @Autowired
    StepBuilderFactory stepBuilderFactory;
    @Autowired
    DataSource dataSource;

    @Bean
    public JdbcCursorItemReader<Employee> employeeJdbcCursorItemReader() {
        JdbcCursorItemReader<Employee> jdbcCursorItemReader = new JdbcCursorItemReader<>();
        jdbcCursorItemReader.setDataSource(dataSource);
        jdbcCursorItemReader.setSql("SELECT employee_id as id, first_name, last_name FROM pps_employee");
        jdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<>(Employee.class));
        return jdbcCursorItemReader;
    }

    @Bean
    public JdbcPagingItemReader<Employee> employeeJdbcPagingItemReader() {
        JdbcPagingItemReader<Employee> reader = new JdbcPagingItemReader<>();
        reader.setDataSource(dataSource);
        reader.setFetchSize(CHUNK_SIZE);
        reader.setRowMapper(new BeanPropertyRowMapper<>(Employee.class));

        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("select employee_id, employee_id as id, first_name, last_name, primary_title");
        queryProvider.setFromClause("from pps_employee");

        Map<String, Order> sortKeys = new HashMap<>();
        sortKeys.put("employee_id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        reader.setQueryProvider(queryProvider);

        return reader;
    }

    @Bean
    public FlatFileItemWriter<Employee> employeeFlatFileItemWriter() {
        FlatFileItemWriter<Employee> employeeFlatFileItemWriter = new FlatFileItemWriter<>();
        employeeFlatFileItemWriter.setResource(new ClassPathResource("employees.csv"));
        employeeFlatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<Employee>() {{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<Employee>() {{
                setNames(new String[] {"id", "firstName", "lastName", "primaryTitle"});
            }});
        }});
        return employeeFlatFileItemWriter;
    }

    @Bean
    public Step loadEmployee() {
        return stepBuilderFactory.get("loadEmployee")
                .<Employee, Employee>chunk(CHUNK_SIZE)
//                .reader(employeeJdbcCursorItemReader())
                .reader(employeeJdbcPagingItemReader())
//                .processor(employee -> {
//                    System.out.printf("%s %s %s %s\n",
//                            employee.getId(),
//                            employee.getFirstName(),
//                            employee.getLastName(),
//                            employee.getPrimaryTitle());
//                    return employee;
//                })
//                .writer(items -> {
//                    for (Employee employee : items) {
//                        System.out.printf("%s %s %s %s\n",
//                            employee.getId(),
//                            employee.getFirstName(),
//                            employee.getLastName(),
//                            employee.getPrimaryTitle());
//                    }
//                })
                .writer(employeeFlatFileItemWriter())
                .build();
    }

    @Bean
    public Job job() {
        logger.info("**** Starting Job 2 ****");
        return jobBuilderFactory.get("job2")
                .incrementer(new RunIdIncrementer())
                .flow(loadEmployee())
                .end()
                .build();
    }
}
