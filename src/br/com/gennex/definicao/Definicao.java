package br.com.gennex.definicao;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.TimerTask;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Logger;

public abstract class Definicao extends TimerTask {

	private static final String nomeArquivoCache = "dbcache.properties";

	private static String nomeArquivoDefinicao;

	private static final String obtemDataModificacaoBanco = "SELECT DATAHORAULTIMAATUALIZACAO FROM GPROPRIEDADESDISCADOR WHERE IDENTIFICADOR = \'%s\'";

	private static final String obtemDefinicoesDoBanco = "SELECT P.PROPRIEDADE, P.VALOR "
			+ "FROM GPROPRIEDADESDISCADOR D INNER JOIN GPROPRIEDADES P ON D.ID = P.IDPROPRIEDADEDISCADOR "
			+ "WHERE D.IDENTIFICADOR = \'%s\'";

	public static final String getNomeArquivoDefinicao() {
		return nomeArquivoDefinicao;
	}

	protected Configuration properties = new PropertiesConfiguration();

	private long ultimaModificacaoDefinicao = 0;

	private Calendar ultimaModificacaoDefinicaoBanco = null;

	protected Definicao() {

	}

	public void atualizaDefinicaoSeNecessario() {
		if (!arquivoExiste()) {
			tarefaPosCarregamentoDefinicao();
			return;
		}
		if (verificaSeArquivoFoiModificadoEAtualizaInstante()) {
			carregaDefinicaoDoArquivo();
			if (getUsaDefinicoesDoBanco()) {
				Connection conn = null;
				try {
					conn = getConnectionDefinicao();
					if (conn == null) {
						Logger.getLogger(getClass()).warn(
								"Conexao nao obtida. Usando cache!");
						carregaCache();
						tarefaPosCarregamentoDefinicao();
						return;
					}
					if (verificaSeBancoFoiModificadoEAtualizaInstante(conn)) {
						carregaDefinicoesDoBanco(conn);
						tarefaPosCarregamentoDefinicao();
						return;
					}
				} finally {
					try {
						try {
							if (conn != null) {
								conn.commit();
								conn.close();
								conn = null;
							}
						} catch (SQLException e) {
							Logger.getLogger(getClass()).error(e.getMessage(),
									e);
						}
					} catch (Exception e) {
						Logger.getLogger(getClass()).error(e.getMessage(), e);
					}
				}
			}
			tarefaPosCarregamentoDefinicao();
			return;
		}

		if (!getUsaDefinicoesDoBanco()) {
			return;
		}
		Connection conn = null;
		try {
			conn = getConnectionDefinicao();
			if (conn == null) {
				Logger.getLogger(getClass()).warn(
						"Conexao nao obtida. Usando cache!");
				carregaCache();
				tarefaPosCarregamentoDefinicao();
				return;
			}
			if (verificaSeBancoFoiModificadoEAtualizaInstante(conn)) {
				carregaDefinicoesDoBanco(conn);
				tarefaPosCarregamentoDefinicao();
				return;
			}
		} finally {
			try {
				try {
					if (conn != null) {
						conn.commit();
						conn.close();
						conn = null;
					}
				} catch (SQLException e) {
					Logger.getLogger(getClass()).error(e.getMessage(), e);
				}
			} catch (Exception e) {
				Logger.getLogger(getClass()).error(e.getMessage(), e);
			}
		}
	}

	private boolean arquivoExiste() {
		File f = new File(nomeArquivoDefinicao);
		return f.exists();
	}

	private void carregaCache() {
		carregaDefinicaoDoArquivo(nomeArquivoCache);
	}

	private void carregaDefinicaoDoArquivo() {
		carregaDefinicaoDoArquivo(getNomeArquivoDefinicao());
	}

	private void carregaDefinicaoDoArquivo(String arquivo) {
		Logger.getLogger(getClass()).info(
				"Carregando definicoes do arquivo " + arquivo + ".");
		Properties defaultProps = new Properties();
		boolean carregou = false;
		for (int i = 0; i < 10; i++) {
			try {
				FileInputStream in = new FileInputStream(arquivo);
				defaultProps.load(in);
				in.close();
				carregou = true;
				break;
			} catch (FileNotFoundException e) {
				Logger
						.getLogger(getClass())
						.error(
								"Arquivo de properties default nao encontrado. Continuarei tentando...");
				try {
					Thread.sleep(5000);
				} catch (InterruptedException f) {
					Logger.getLogger(getClass()).error(f.getMessage(), f);
				}
			} catch (Exception e) {
				Logger.getLogger(getClass()).error(e.getMessage(), e);
			}
		}
		if (!carregou) {
			Logger
					.getLogger(getClass())
					.error(
							"Nao consegui encontrar arquivo de properties. Encerrando!");
			System.exit(1);
		}

		synchronized (properties) {
			Iterator<Object> it = defaultProps.keySet().iterator();
			while (it.hasNext()) {
				String strValores = (String) it.next();
				properties.setProperty(strValores, (String) defaultProps
						.get(strValores));
			}
		}
	}

	private void carregaDefinicoesDoBanco(Connection conn) {
		Logger.getLogger(getClass()).info("Carregando definicoes do banco.");
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			try {
				String sql = String.format(obtemDefinicoesDoBanco,
						getIdentificadorDefinicao());
				ps = conn.prepareStatement(sql);
				rs = ps.executeQuery();
				synchronized (properties) {
					while (rs.next()) {
						properties.setProperty(rs.getString("PROPRIEDADE"), rs
								.getString("VALOR"));
					}
					salvaCache((PropertiesConfiguration) properties,
							nomeArquivoCache);
				}

			} catch (SQLException e) {
				Logger.getLogger(getClass()).error(e.getMessage(), e);
			}
		} finally {
			try {
				try {
					if (rs != null) {
						rs.close();
						rs = null;
					}
				} catch (Exception e) {
					Logger.getLogger(getClass()).error(e.getMessage(), e);
				}
				try {
					if (ps != null) {
						ps.close();
						ps = null;
					}
				} catch (Exception e) {
					Logger.getLogger(getClass()).error(e.getMessage(), e);
				}
			} catch (Exception e) {
				Logger.getLogger(getClass()).error(e.getMessage(), e);
			}
		}
	}

	private Connection getConnectionDefinicao() {
		return getConnectionDefinicao(getUsernameBdDefinicao(),
				getPasswordBdDefinicao());

	}

	private Connection getConnectionDefinicao(String usernameBdDefinicao,
			String passwordBdDefinicao) {
		Connection result = null;
		try {
			Class.forName(getClassForNameDefinicao());
		} catch (Exception e) {
			Logger.getLogger(getClass()).error(e.getMessage(), e);
		}
		try {
			result = DriverManager.getConnection(
					getConnectionStringDefinicao(), usernameBdDefinicao,
					passwordBdDefinicao);
			result.setAutoCommit(false);
		} catch (SQLException e) {
			Logger.getLogger(getClass()).error(e.getMessage(), e);
		}
		return result;
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

	public int getPortaBdDefinicao() {
		synchronized (properties) {
			return properties.getInt("mailing.portaBdDefinicao", 1521);
		}
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

	private String getServidorBdDefinicao() {
		synchronized (properties) {
			return properties.getString("sistema.servidorBdDefinicao",
					"127.0.0.1");
		}
	}

	private boolean getUsaDefinicoesDoBanco() {
		synchronized (properties) {
			return properties
					.getString("sistema.usaDefinicoesDoBanco", "False")
					.equalsIgnoreCase("True");
		}
	}

	private String getUsernameBdDefinicao() {
		synchronized (properties) {
			return properties.getString("sistema.usernameBdDefinicao",
					"discador");
		}
	}

	@Override
	public final void run() {
		atualizaDefinicaoSeNecessario();
	}

	private void salvaCache(PropertiesConfiguration propriedades, String destino) {
		try {
			propriedades.save(destino);
		} catch (ConfigurationException e) {
			Logger.getLogger(getClass()).error(e.getMessage(), e);
		}
	}

	public final void setNomeArquivoDefinicao(String nomeArquivoDefinicao) {
		Definicao.nomeArquivoDefinicao = nomeArquivoDefinicao;
	}

	protected abstract void tarefaPosCarregamentoDefinicao();

	private boolean verificaSeArquivoFoiModificadoEAtualizaInstante() {
		File f = new File(nomeArquivoDefinicao);
		if (f.lastModified() != ultimaModificacaoDefinicao) {
			ultimaModificacaoDefinicao = f.lastModified();
			return true;
		} else
			return false;
	}

	private boolean verificaSeBancoFoiModificadoEAtualizaInstante(
			Connection conn) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			String sql = String.format(obtemDataModificacaoBanco,
					getIdentificadorDefinicao());
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();

			if (!rs.next()) {
				Logger.getLogger(getClass()).fatal(
						"Identificador de definicao nao encontrado!"
								+ getIdentificadorDefinicao());
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					Logger.getLogger(getClass()).error(e.getMessage(), e);
				}
				System.exit(1);
				// return false;
			}

			Date ultimaModificacao = rs
					.getTimestamp("DATAHORAULTIMAATUALIZACAO");
			Calendar calUltimaModificacao = Calendar.getInstance();
			calUltimaModificacao.setTime(ultimaModificacao);

			if (calUltimaModificacao.equals(ultimaModificacaoDefinicaoBanco)) {
				return false;
			}

			ultimaModificacaoDefinicaoBanco = calUltimaModificacao;

			return true;
		} catch (SQLException e) {
			Logger.getLogger(getClass()).error(e.getMessage(), e);
			return false;
		} finally {
			try {
				try {
					if (rs != null) {
						rs.close();
						rs = null;
					}
				} catch (Exception e) {
					Logger.getLogger(getClass()).error(e.getMessage(), e);
				}
				try {
					if (ps != null) {
						ps.close();
						ps = null;
					}
				} catch (Exception e) {
					Logger.getLogger(getClass()).error(e.getMessage(), e);
				}

			} catch (Exception e) {
				Logger.getLogger(getClass()).error(e.getMessage(), e);
			}
		}

	}

	protected static final int TIPO_BANCO_ORACLE = 0;

	protected static final int TIPO_BANCO_MYSQL = 1;

	protected static final int TIPO_BANCO_MSSQL = 2;

	protected static final String strTipoBancoOracle = "Oracle";

	protected static final String strTipoBancoMySql = "MySQL";

	protected static final String strTipoBancoMsSql = "MSSQL";

	private String getClassForNameDefinicao() {
		switch (getTipoBancoDefinicao()) {
		case TIPO_BANCO_MYSQL:
			return "com.mysql.jdbc.Driver";
		case TIPO_BANCO_MSSQL:
			return "net.sourceforge.jtds.jdbc.Driver";
		}
		return "oracle.jdbc.driver.OracleDriver";
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

}
