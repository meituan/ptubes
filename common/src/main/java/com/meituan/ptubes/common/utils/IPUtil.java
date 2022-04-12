package com.meituan.ptubes.common.utils;

import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

public class IPUtil {
	private static final Logger LOG = LoggerFactory.getLogger(IPUtil.class);

	private static final String IP_FORMAT = "%d.%d.%d.%d";

	private static final List<String> IGNORE_NI_NAMES = initIgnoreNiNames();

	public static String longToIp(Long number, boolean isBigEndian) {
		long[] ipNum = new long[4];
		String ip = "";
		if (isBigEndian) {
			for (int i = 0; i <= 3; i++) {
				ipNum[i] = number & 0xff;
				number = number >>> 8;
			}
		} else {
			for (int i = 3; i >= 0; i--) {
				ipNum[i] = number & 0xff;
				number = number >>> 8;
			}
		}
		ip = String.format(IP_FORMAT, ipNum[0], ipNum[1], ipNum[2], ipNum[3]);
		return ip;
	}

	public static long ipToLong(String ip) {
		String[] ipArray = ip.split("\\.");
		List ipNums = new ArrayList();
		for (int i = 0; i < 4; ++i) {
			ipNums.add(Long.valueOf(Long.parseLong(ipArray[i].trim())));
		}
		return ((Long) ipNums.get(0)).longValue() * 256L * 256L * 256L
				+ ((Long) ipNums.get(1)).longValue() * 256L * 256L + ((Long) ipNums.get(2)).longValue() * 256L
				+ ((Long) ipNums.get(3)).longValue();
	}

	private static List<String> initIgnoreNiNames() {
		List<String> ret = new ArrayList<String>();
		ret.add("vnic");
		ret.add("docker");
		ret.add("vmnet");
		ret.add("vmbox");
		ret.add("vbox");
		return ret;
	}

	private static boolean isIgnoreNI(String niName) {
		for (String item : IGNORE_NI_NAMES) {
			if (StringUtils.containsIgnoreCase(niName, item)) {
				return true;
			}
		}
		return false;
	}

	public static String getIpV4() {
		String ip = "";
		Enumeration<NetworkInterface> networkInterface;
		try {
			networkInterface = NetworkInterface.getNetworkInterfaces();
		} catch (SocketException e) {
			LOG.error("fail to get network interface information.", e);
			return ip;
		}
		Set<String> ips = new HashSet<String>();
		while (networkInterface.hasMoreElements()) {
			NetworkInterface ni = networkInterface.nextElement();
			// Ignore the IP of the virtual network card, the IP of the docker container
			String niName = (null != ni) ? ni.getName() : "";
			if (isIgnoreNI(niName)) {
				continue;
			}

			Enumeration<InetAddress> inetAddress = null;
			try {
				if (null != ni) {
					inetAddress = ni.getInetAddresses();
				}
			} catch (Exception e) {
				LOG.debug("fail to get ip information.", e);
			}
			while (null != inetAddress && inetAddress.hasMoreElements()) {
				InetAddress ia = inetAddress.nextElement();
				if (ia instanceof Inet6Address) {
					continue; // ignore ipv6
				}
				String thisIp = ia.getHostAddress();
				// exclude loopback addresses
				if (!ia.isLoopbackAddress() && !thisIp.contains(":") && !"127.0.0.1".equals(thisIp)) {
					ips.add(thisIp);
					if (StringUtils.isBlank(ip)) {
						ip = thisIp;
					}
				}
			}
		}

		// Bind two IPs for the new office cloud host, only use its 10 segments of IP
		if (ips.size() >= 2) {
			for (String str : ips) {
				if (str.startsWith("10.")) {
					ip = str;
					break;
				}
			}
		}

		if (StringUtils.isBlank(ip)) {
			LOG.error("cannot get local ip.");
			ip = "";
		}

		return ip;
	}

}
