package br.com.gennex.definicao;

import java.io.FileNotFoundException;
import java.sql.SQLException;

import javax.sql.DataSource;

import net.sourceforge.jtds.jdbcx.JtdsDataSource;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.DatabaseConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.event.ConfigurationEvent;
import org.apache.commons.configuration.event.ConfigurationListener;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.log4j.Logger;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public abstract class Definicao {

	protected Configuration properties = new CompositeConfiguration();

	protected static final int TIPO_BANCO_ORACLE = 0;

	protected static final int TIPO_BANCO_MYSQL = 1;

	protected static final int TIPO_BANCO_MSSQL = 2;

	protected static final String strTipoBancoOracle = "Oracle";

	protected static final String strTipoBancoMySql = "MySQL";

	protected static final String strTipoBancoMsSql = "MSSQL";

	private DataSource criaDataSourceMsSql(String userName, String password) {
		DataSource dataSource = new JtdsDataSource();
		((JtdsDataSource) dataSource).setServerName(getServidorBdDefinicao());
		((JtdsDataSource) dataSource).setPortNumber(getPortaBdDefinicao());
		((JtdsDataSource) dataSource).setDatabaseName(getNomeBdDefinicao());
		((JtdsDataSource) dataSource).setUser(userName);
		((JtdsDataSource) dataSource).setPassword(password);
		return dataSource;
	}

	private DataSource criaDataSourceMysql(String userName, String password) {
		DataSource dataSource = new MysqlDataSource();
		((MysqlDataSource) dataSource).setURL(getConnectionStringDefinicao());
		((MysqlDataSource) dataSource).setServerName(getServidorBdDefinicao());
		((MysqlDataSource) dataSource).setPortNumber(getPortaBdDefinicao());
		((MysqlDataSource) dataSource).setDatabaseName(getNomeBdDefinicao());
		((MysqlDataSource) dataSource).setUser(userName);
		((MysqlDataSource) dataSource).setPassword(password);
		return dataSource;
	}

	private DataSource criaDataSourceOracle(String userName, String password)
			throws SQLException {
		DataSource dataSource = new OracleDataSource();
		((OracleDataSource) dataSource).setURL(getConnectionStringDefinicao());
		((OracleDataSource) dataSource).setDriverType("thin");
		((OracleDataSource) dataSource).setServerName(getServidorBdDefinicao());
		((OracleDataSource) dataSource).setPortNumber(getPortaBdDefinicao());
		((OracleDataSource) dataSource).setServiceName(getNomeBdDefinicao());
		((OracleDataSource) dataSource).setUser(userName);
		((OracleDataSource) dataSource).setPassword(password);
		return dataSource;
	}

	private String getConnectionStringDefinicao() {
		switch (getTipoBancoDefinicao()) {
		case TIPO_BANCO_MYSQL:
			return "jdbc:mysql://" + getServidorBdDefinicao() + "/"
					+ getNomeBdDefinicao();
		case TIPO_BANCO_MSSQL:
			return "jdbc:jtds:sqlserver://" + getServidorBdDefinicao()
					+ ";databaseName=" + getNomeBdDefinicao();
		}
		return "jdbc:oracle:thin:@" + getServidorBdDefinicao() + ":"
				+ getPortaBdDefinicao() + ":" + getNomeBdDefinicao();

	}

	private String getIdentificadorDefinicao() {
		synchronized (properties) {
			return properties.getString("sistema.identificadorDefinicao", "");
		}
	}

	private String getNomeBdDefinicao() {
		synchronized (properties) {
			return properties.getString("sistema.nomeBdDefinicao", "gennex");
		}
	}

	private String getPasswordBdDefinicao() {
		synchronized (properties) {
			return properties.getString("sistema.passwordBdDefinicao",
					"discad0r");
		}
	}

	public int getPortaBdDefinicao() {
		synchronized (properties) {
			return properties.getInt("mailing.portaBdDefinicao", 1521);
		}
	}

	private String getServidorBdDefinicao() {
		synchronized (properties) {
			return properties.getString("sistema.servidorBdDefinicao",
					"127.0.0.1");
		}
	}

	private int getTipoBancoDefinicao() {
		synchronized (properties) {
			if (properties.getString("mailing.tipoBancoDefinicao",
					strTipoBancoOracle).equalsIgnoreCase(strTipoBancoMySql))
				return TIPO_BANCO_MYSQL;
			if (properties.getString("mailing.tipoBancoDefinicao",
					strTipoBancoOracle).equalsIgnoreCase(strTipoBancoMsSql))
				return TIPO_BANCO_MSSQL;
		}
		return TIPO_BANCO_ORACLE;
	}

	private boolean getUsaDefinicoesDoBanco() {
		synchronized (properties) {
			return properties.getBoolean("sistema.usaDefinicoesDoBanco", false);
		}
	}

	private String getUsernameBdDefinicao() {
		synchronized (properties) {
			return properties.getString("sistema.usernameBdDefinicao",
					"discador");
		}
	}

	public void inicializaDefinicao(String nomeArquivoDefinicao)
			throws SQLException, FileNotFoundException {
		try {
			PropertiesConfiguration propFile = new PropertiesConfiguration(
					nomeArquivoDefinicao);
			propFile.setReloadingStrategy(new FileChangedReloadingStrategy());
			propFile.addConfigurationListener(new ConfigurationListener() {
				@Override
				public void configurationChanged(ConfigurationEvent event) {
					if (event.isBeforeUpdate())
						return;

					if (event.getType() != AbstractFileConfiguration.EVENT_RELOAD)
						return;
					
					Logger.getLogger(getClass()).info(
							"Carregando definicoes do arquivo.");
					tarefaPosCarregamentoDefinicao();
				}
			});
			((CompositeConfiguration) properties).addConfiguration(propFile);
			tarefaPosCarregamentoDefinicao();

			if (!getUsaDefinicoesDoBanco())
				return;

			((CompositeConfiguration) properties)
					.addConfiguration(new DatabaseConfiguration(
							obtemDataSource(),
							"GPROPRIEDADESDISCADOR D INNER JOIN GPROPRIEDADES P ON D.ID = P.IDPROPRIEDADEDISCADOR",
							"D.IDENTIFICADOR", "P.PROPRIEDADE", "P.VALOR",
							getIdentificadorDefinicao()));

		} catch (ConfigurationException e) {
			throw new FileNotFoundException(e.getMessage());
		}
	}

	private DataSource obtemDataSource() throws SQLException {
		switch (getTipoBancoDefinicao()) {
		case TIPO_BANCO_MYSQL:
			return criaDataSourceMysql(getUsernameBdDefinicao(),
					getPasswordBdDefinicao());
		case TIPO_BANCO_MSSQL:
			return criaDataSourceMsSql(getUsernameBdDefinicao(),
					getPasswordBdDefinicao());
		default:
			return criaDataSourceOracle(getUsernameBdDefinicao(),
					getPasswordBdDefinicao());
		}
	}

	protected abstract void tarefaPosCarregamentoDefinicao();

}
