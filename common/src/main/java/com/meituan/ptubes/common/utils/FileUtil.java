package com.meituan.ptubes.common.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class FileUtil {
	private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

	public static List<String> readFile(File file) {
		List<String> res = new ArrayList<>();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(file));
			String line;
			while ((line = reader.readLine()) != null) {
				res.add(line);
			}
			return res;
		} catch (Exception e) {
			LOG.error(e.getMessage());
			return res;
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					LOG.error(e.getMessage());
				}
			}
		}
	}

}
