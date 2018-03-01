package com.danikula.videocache.sourcestorage;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

import com.danikula.videocache.SourceInfo;
import com.danikula.videocache.file.FileCache;
import com.danikula.videocache.file.FileNameGenerator;
import com.danikula.videocache.file.Md5FileNameGenerator;

import java.io.File;

import static com.danikula.videocache.Preconditions.checkAllNotNull;
import static com.danikula.videocache.Preconditions.checkNotNull;

/**
 * Database based {@link SourceInfoStorage}.
 *
 * @author Alexey Danilov (danikula@gmail.com).
 * @author zhangfeng
 * @license: Apache License 2.0
 */
class DatabaseSourceInfoStorage extends SQLiteOpenHelper implements SourceInfoStorage {
    private String TAG = "DatabaseSourceInfoStorage";
    private static final String TABLE = "SourceInfo";
    private static final String COLUMN_ID = "_id";
    private static final String COLUMN_TITLE = "title";
    private static final String COLUMN_URL = "url";
    private static final String COLUMN_LENGTH = "length";
    private static final String COLUMN_MIME = "mime";
    private static final String COLUMN_REQUEST_SIZE = "request";
    private static final String[] ALL_COLUMNS = new String[]{COLUMN_ID, COLUMN_TITLE, COLUMN_URL, COLUMN_LENGTH, COLUMN_MIME, COLUMN_REQUEST_SIZE};
    private static final String CREATE_SQL =
            "CREATE TABLE " + TABLE + " (" +
                    COLUMN_ID + " INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL," +
                    COLUMN_TITLE + " TEXT, " +
                    COLUMN_URL + " TEXT NOT NULL," +
                    COLUMN_MIME + " TEXT," +
                    COLUMN_LENGTH + " INTEGER," +
                    COLUMN_REQUEST_SIZE + " INTEGER" +
                    ");";
    private static final String DROP_TABLE = "DROP TABLE " + TABLE + ";";
    private FileNameGenerator generator = new Md5FileNameGenerator();
    private Context context;

    DatabaseSourceInfoStorage(Context context) {
        super(context, "AndroidVideoCache.db", null, 2);
        checkNotNull(context);
        this.context = context;
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        checkNotNull(db);
        db.execSQL(CREATE_SQL);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        Log.w(TAG, "onUpgrade " + oldVersion + "->" + newVersion);
        deleteCache();
        db.beginTransaction();
        try {
            db.execSQL(DROP_TABLE);
            db.execSQL(CREATE_SQL);
            db.setTransactionSuccessful();
        } finally {
            db.endTransaction();
        }
    }

    private void deleteCache() {
        new Thread() {
            @Override
            public void run() {
                Cursor cursor = null;
                File cacheDir = context.getFilesDir();
                try {
                    cursor = getReadableDatabase().rawQuery("select " + COLUMN_URL + " from " + TABLE, null);
                    if (cursor != null) {
                        while (cursor.moveToNext()) {
                            String name = generator.generate(cursor.getString(cursor.getColumnIndex(COLUMN_URL)));
                            File file = new File(cacheDir, name);
                            File unFile = new File(cacheDir, name + FileCache.TEMP_POSTFIX);
                            if (file.exists()) {
                                file.delete();
                                Log.w(TAG, "clear " + file.getName());
                            }
                            if (unFile.exists()) {
                                unFile.delete();
                                Log.w(TAG, "clear " + unFile.getName());
                            }
                        }
                    }
                } finally {
                    if (cursor != null) {
                        cursor.close();
                    }
                }
            }
        }.start();
    }

    @Override
    public SourceInfo get(String url) {
        checkNotNull(url);
        Cursor cursor = null;
        try {
            cursor = getReadableDatabase().query(TABLE, ALL_COLUMNS, COLUMN_URL + "=?", new String[]{url}, null, null, null);
            return cursor == null || !cursor.moveToFirst() ? null : convert(cursor);
        } catch (Exception e) {
            Log.w(getClass().getSimpleName(), e.getMessage());
            e.printStackTrace();
            return null;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    @Override
    public void put(String url, SourceInfo sourceInfo) {
        checkAllNotNull(url, sourceInfo);
        SourceInfo sourceInfoFromDb = get(url);
        boolean exist = sourceInfoFromDb != null;
        ContentValues contentValues = convert(sourceInfo);
        if (exist) {
            getWritableDatabase().update(TABLE, contentValues, COLUMN_URL + "=?", new String[]{url});
        } else {
            getWritableDatabase().insert(TABLE, null, contentValues);
        }
    }

    @Override
    public void release() {
        close();
    }

    private SourceInfo convert(Cursor cursor) {
        return new SourceInfo(
                cursor.getString(cursor.getColumnIndexOrThrow(COLUMN_TITLE)),
                cursor.getString(cursor.getColumnIndexOrThrow(COLUMN_URL)),
                cursor.getLong(cursor.getColumnIndexOrThrow(COLUMN_LENGTH)),
                cursor.getString(cursor.getColumnIndexOrThrow(COLUMN_MIME)),
                cursor.getLong(cursor.getColumnIndexOrThrow(COLUMN_REQUEST_SIZE))
        );
    }

    private ContentValues convert(SourceInfo sourceInfo) {
        ContentValues values = new ContentValues();
        values.put(COLUMN_TITLE, sourceInfo.title);
        values.put(COLUMN_URL, sourceInfo.url);
        values.put(COLUMN_LENGTH, sourceInfo.length);
        values.put(COLUMN_MIME, sourceInfo.mime);
        values.put(COLUMN_REQUEST_SIZE, sourceInfo.requestSize);
        return values;
    }
}
