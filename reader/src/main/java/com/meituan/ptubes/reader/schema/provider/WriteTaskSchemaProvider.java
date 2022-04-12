package com.meituan.ptubes.reader.schema.provider;

import com.meituan.ptubes.common.exception.PtubesRunTimeException;
import com.meituan.ptubes.common.log.Logger;
import com.meituan.ptubes.common.log.LoggerFactory;
import com.meituan.ptubes.reader.container.common.lifecycle.AbstractLifeCycle;
import com.meituan.ptubes.reader.schema.common.SchemaVersion;
import com.meituan.ptubes.reader.schema.common.VirtualSchema;
import com.meituan.ptubes.reader.schema.service.ISchemaService;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Shared by all writers accessing a readTask (or only by writers that read the same service group)
 */
public class WriteTaskSchemaProvider extends AbstractLifeCycle {

	private static final Logger LOG = LoggerFactory.getLogger(WriteTaskSchemaProvider.class);

	private final String readTaskName;
	private final ISchemaService schemaService;
	/** Use reference counting **/
	private final Map<String/* tableFullName */, Map<SchemaVersion, ReferenceCountedVersionedSchema>> schemaCache = new HashMap<>();
	private final ReentrantReadWriteLock rwLock;
	private final ReentrantReadWriteLock.ReadLock readLock;
	private final ReentrantReadWriteLock.WriteLock writeLock;

	public WriteTaskSchemaProvider(String readTaskName, ISchemaService schemaService) {
		this.readTaskName = readTaskName;
		this.schemaService = schemaService;
		this.rwLock = new ReentrantReadWriteLock();
		this.readLock = this.rwLock.readLock();
		this.writeLock = this.rwLock.writeLock();
	}

	@Override
	public void doStart() {
		try {
			this.writeLock.lockInterruptibly();
			try {
				
				schemaCache.put(VirtualSchema.INTERNAL_TABLE_SCHEMA.getVersion()
									.getTableFullName(), new HashMap<SchemaVersion, ReferenceCountedVersionedSchema>() {{
					put(VirtualSchema.INTERNAL_TABLE_SCHEMA.getVersion(),
						new ReferenceCountedVersionedSchema(self(), VirtualSchema.INTERNAL_TABLE_SCHEMA));
				}});
			} catch (Exception e) {
				LOG.error("writer schema provider of reader task {} start error", readTaskName, e);
				throw new PtubesRunTimeException("load schema for writer tasks error", e);
			} finally {
				this.writeLock.unlock();
			}
		} catch (InterruptedException ie) {
			LOG.error("initialization of writer schema provider is interrupted", ie);
			throw new PtubesRunTimeException("initialization of writer schema provider is interrupted", ie);
		}
	}

	@Override
	public void doStop() {
		try {
			this.writeLock.lockInterruptibly();
			try {
				this.schemaCache.clear();
			} finally {
				this.writeLock.unlock();
			}
		} catch (InterruptedException ie) {
			LOG.error("Release process of writer schema provider is interrupted", ie);
			throw new PtubesRunTimeException("Release process of writer schema provider is interrupted", ie);
		}
	}

	public WriteTaskSchemaProvider self() {
		return this;
	}

	public boolean releaseSchema(ReferenceCountedVersionedSchema refCntVersionedSchema) throws InterruptedException {
		SchemaVersion schemaVersion = refCntVersionedSchema.getVersionedSchema().getVersion();
		String tableFullName = schemaVersion.getTableFullName();

		
		this.writeLock.lockInterruptibly();
		try {
			boolean needRemove = refCntVersionedSchema.refCntRelease();
			LOG.debug("schema {} release, refCnt={}", schemaVersion.toString(), refCntVersionedSchema.refCnt());
			if (needRemove) {
				// release object
				if (schemaCache.containsKey(tableFullName)) {
					Map<SchemaVersion, ReferenceCountedVersionedSchema> versionedSchemaMap = schemaCache.get(tableFullName);
					if (versionedSchemaMap.containsKey(schemaVersion)) {
						versionedSchemaMap.remove(schemaVersion);
					}

					if (versionedSchemaMap.size() == 0) {
						schemaCache.remove(tableFullName);
					}
				}
				LOG.debug("schema {} remove from schemaProvider", schemaVersion);
			}

			return true;
		} finally {
			this.writeLock.unlock();
		}
	}

	public void clear() {
		this.writeLock.lock();
		try {
			this.schemaCache.clear();
		} finally {
			this.writeLock.unlock();
		}
	}

}
