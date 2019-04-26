package io.pivotal.pal.tracker;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.List;

import static java.sql.Statement.RETURN_GENERATED_KEYS;

public class JdbcTimeEntryRepository implements  TimeEntryRepository{
    private final JdbcTemplate jdbcTemplate;

    public JdbcTimeEntryRepository(DataSource dataSource) {
        jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public TimeEntry create(TimeEntry timeEntry) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        String sql = "INSERT INTO time_entries (project_id, user_id, date, hours) VALUES (?, ?, ?, ?)";
        jdbcTemplate.update( connection -> {
            PreparedStatement statement = connection.prepareStatement(sql,RETURN_GENERATED_KEYS);
                    statement.setLong(1, timeEntry.getProjectId());
                    statement.setLong(2, timeEntry.getUserId());
                    statement.setDate(3, Date.valueOf(timeEntry.getDate()));
                    statement.setInt(4, timeEntry.getHours());
                    return  statement;
        },keyHolder);

        return find(keyHolder.getKey()!=null?keyHolder.getKey().longValue():1L);
    }

    @Override
    public TimeEntry find(Long id) {
        String sql = "SELECT id, project_id, user_id, date, hours FROM time_entries WHERE id = ?";
        return jdbcTemplate.query(sql,new Object[]{id},extractor);
    }

    @Override
    public List<TimeEntry> list() {
        String sql = "SELECT id, project_id, user_id, date, hours FROM time_entries";
        return jdbcTemplate.query(sql,mapper);
    }

    @Override
    public TimeEntry update(Long id, TimeEntry timeEntry) {
        String sql = "update time_entries set project_id =?, user_id=?, date=?, hours=? where id=?";
        jdbcTemplate.update( connection -> {
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setLong(1, timeEntry.getProjectId());
            statement.setLong(2, timeEntry.getUserId());
            statement.setDate(3, Date.valueOf(timeEntry.getDate()));
            statement.setInt(4, timeEntry.getHours());
            statement.setLong(5,id);
            return  statement;
        });
        return find(id);
    }

    @Override
    public void delete(Long id) {
        String sql = "delete from time_entries where id =?";
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setLong(1,id);
            return  statement;
        });
    }

    private final RowMapper<TimeEntry> mapper = (rs, rowNum) -> new TimeEntry(
            rs.getLong("id"),
            rs.getLong("project_id"),
            rs.getLong("user_id"),
            rs.getDate("date").toLocalDate(),
            rs.getInt("hours")
    );

    private final ResultSetExtractor<TimeEntry> extractor =
            (rs) -> rs.next() ? mapper.mapRow(rs,1) : null;
}
