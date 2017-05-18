package skyepg;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import beans.GenreBean;
import commons.MyException;
import commons.Utils;
import database.DbException;
import database.EpgDatabase;
import database.EpgDatabase.GenresEnabled;
import net.GMail;
import net.Internet;
import skyresponses.SkyChannel;
import skyresponses.SkyChannelPlan;
import skyresponses.SkyEvent;
import skyresponses.SkyEventDescr;
import skyresponses.SkyGenre;

public class SkyEpg {

	// --------------------------------------------------------------------------------------------
	// Constants
	// --------------------------------------------------------------------------------------------
	final static Logger logger = LogManager.getLogger(SkyEpg.class);
	private final String URL_GENRE =  "http://guidatv.sky.it/app/guidatv/contenuti/data/grid/grid_%s_channels.js"; // genre
	private final String URL_CANALE = "http://guidatv.sky.it/app/guidatv/contenuti/data/grid/%s/ch_%d.js";	// YY_MM_DD, chn id
	private final String URL_TRAMA =  "http://guidatv.sky.it/EpgBackend/event_description.do?eid=%d";	// id
	//private final String URL_MENSILE = "http://guidatv.sky.it/EpgBackend/getprogrammazione.do?idprogramma=%d";	// pid
	private final String myName = SkyEpg.class.getSimpleName();

	// --------------------------------------------------------------------------------------------
	// Variables
	// --------------------------------------------------------------------------------------------
	private EpgDatabase db;
	private int dayStart, dayStop;
	private LocalDateTime now;
	private DateTimeFormatter skyDateFormatter, dateFormatter;
	private String senderUsr, senderPwd, destMail;
	private String curGenre, curChnName, curDate;
	private String msg;
	private int curChnNum, curEvent;
	private GMail gmail;

	public static void main(String[] args) {
		new SkyEpg();
	}

	private SkyEpg() {
		now = LocalDateTime.now();
		skyDateFormatter = DateTimeFormatter.ofPattern("yy_MM_dd");
		dateFormatter = DateTimeFormatter.ofPattern("yy_MM_dd HH:mm");
		List<String> result = new ArrayList<String>();
		
		logger.info("PROCESS START");
		long mainStartTime = System.currentTimeMillis();
		
		try {
			// connects to the DB and gets the parameters
			db = new EpgDatabase("skyepg.properties");
			db.logAdd(myName, "PROCESS START");
			dayStart  = db.getParamAsInteger("DAY_START");
			dayStop   = db.getParamAsInteger("DAY_STOP");
			senderUsr = db.getParamAsString("EMAIL_FROM_USR");
			senderPwd = db.getParamAsString("EMAIL_FROM_PWD");
			destMail  = db.getParamAsString("EMAIL_TO");
			gmail = new GMail(senderUsr, senderPwd);
			gmail.Send(destMail, myName, "PROCESS START");
			
			// pre: archives old data + prepares the tables
			db.archiver();
			
			// gets the enabled genres to process
			List<GenreBean> genres = db.genresList(GenresEnabled.YES, 0);
			for (GenreBean genre : genres) {
				// resetCurValues
				curGenre = genre.getName();
				curChnName = curDate = null;
				curChnNum = curEvent = -1;
				processGenre(genre);
			}
			
			// post: fixes the data
			db.fixer();
			
		} catch (DbException | MyException e) {
			msg = e.getMessage();              result.add(msg); logger.fatal(msg);
			msg = "curGenre: "   + curGenre;   result.add(msg); logger.error(msg);
			msg = "curChnName: " + curChnName; result.add(msg); logger.fatal(msg);
			msg = "curChnNum: "  + curChnNum;  result.add(msg); logger.fatal(msg);
			msg = "curDate: "    + curDate;    result.add(msg); logger.fatal(msg);
			msg = "curChnNum: "  + curEvent;   result.add(msg); logger.fatal(msg);
		} finally {
			msg = "PROCESS END in " + Utils.elapsedTime(mainStartTime);
			result.add(msg);
			// stores the result into the DB and closes the DB connection
			try {
				db.logAdd(myName, result);
			} catch (DbException e) {
				// 'msg' must be leaved unchanged
				result.add(e.getMessage());
				logger.fatal(e.getMessage());
			}
			// sends the result by email
			try {
				if (gmail != null)
					gmail.Send(destMail, myName, result);
			} catch (MyException e) {
				logger.error(e.getMessage());
			}
			logger.info(msg);
		}
	}

	/**
	 * Process a given genre
	 * 
	 * @param genre Genre to be processed
	 * @throws EpgException
	 */
	private void processGenre(GenreBean genre) throws DbException {
		String msg;
		logger.info(genre.getName());
		long startTime = System.currentTimeMillis();
	    try {
	    	// gets the genre's channels list from internet and maps it into a data structure
		    String url = String.format(URL_GENRE, genre.getName().toLowerCase());
			String json = Internet.GetUrl(url, "{\"channels\":", "}");
			SkyGenre skyGenre = jsonMapper(json, SkyGenre.class);
	    	if (skyGenre != null) {
				// stores the json data into the DB
				db.genresUpdateJson(genre.getId(), json);
	    		// extracts the channels list to be processedn
	    		List<SkyChannel> channels = skyGenre.getChannels();
	    		int chnNum = channels.size();
	    		int chnIdx = 1;
				for (SkyChannel chn : channels) {
					curChnName = chn.getName();
					curChnNum = chn.getNumber();
					msg = String.format("...%2d/%2d %4d %s", chnIdx, chnNum, curChnNum, curChnName);
					if (db.skipchannelsCheck(curChnNum, curChnName)) {
						logger.info(msg + ": skipped");
					} else {
						logger.info(msg);
						processChannel(genre.getId(), chn);
					}
					chnIdx += 1;
				}
	    	} else
				logger.error("Null json or unable to map it into SkyGenre");
		} catch (MyException e) {
			throw new DbException(e.getMessage());
		}
		logger.info("{} end in {}", genre.getName(), Utils.elapsedTime(startTime));
	}

	/**
	 * Process a channel
	 * 
	 * @param chnIdx
	 * @param chnNum
	 * @param idGenre
	 * @param chn
	 */
	private void processChannel(int idGenre, SkyChannel chn) {
		byte[] chnLogo = null;

		// gets the channel logo
		try {
			chnLogo = Internet.GetImage(chn.getLogomsite());
		} catch (MyException e) {
			logger.error("Unable to get the channel logo {}", e.getMessage());
		}
		
		try {
			// updates the channels table inserting/updating this channel;
			db.channelsInsertUpdate(idGenre, chn, chnLogo);
			int chnId = chn.getId();
			// loops for all desired days
			String url, json;
			for (int dd = dayStart; dd <= dayStop; dd++) {
				String lastTime = "";
				LocalDateTime day = now.plusDays(dd);
				curDate = day.format(skyDateFormatter);
				logger.info("    {}", curDate);
				try {
					url = String.format(URL_CANALE, curDate, chnId);
					json = Internet.GetUrl(url, "", "");
					SkyChannelPlan chnPlan = jsonMapper(json, SkyChannelPlan.class);
			    	if (chnPlan != null) {
			    		db.channelsUpdateJson(chnId, dd, json);
			    		// banned ???
						for (SkyEvent skyEvent : chnPlan.getPlan()) {
							curEvent = skyEvent.getId();
							if (curEvent != -1) {
								url = String.format(URL_TRAMA, curEvent);
								json = Internet.GetUrl(url, "", "");
								SkyEventDescr skyEvDescr = jsonMapper(json, SkyEventDescr.class);
								String evDescr = skyEvDescr == null ? "" : skyEvDescr.getDescription();
								String startTime = skyEvent.getStarttime();
								LocalDateTime dateTime = LocalDateTime.parse(curDate+" "+startTime, dateFormatter);
								if (startTime.compareTo(lastTime) < 0)
									dateTime = dateTime.plusDays(1);
								else
									lastTime = startTime;
								db.eventsInsert(skyEvent, chnId, dateTime, evDescr);
							}
						}
			    	} else
						logger.error("Null json or unable to map it into SkyChannelPlan");
				} catch (MyException e) {
					throw new DbException(e.getMessage());
				}
			}
		} catch (DbException e) {
			logger.error("Unable to get channel image {}", e.getMessage());
		}
	}
	
	private <T> T jsonMapper(String json, Class<T> mapClass) throws DbException {
		if (json == null) {
			return null;
		} else {
			try {
				ObjectMapper mapper = new ObjectMapper();
				return mapper.readValue(json, mapClass);
			} catch (IOException e) {
				throw new DbException(e.getMessage());
			}
		}
	}
}
