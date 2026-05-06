<script setup>
import { computed, onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { ArrowDown } from '@element-plus/icons-vue'
import api from '../api/client'

const loading = ref(false)
const items = ref([])
const retentionDays = ref(7)
const pagination = ref({ page: 1, pageSize: 12, total: 0 })

const isAdmin = computed(() => {
  try {
    const u = JSON.parse(localStorage.getItem('user') || 'null')
    return Array.isArray(u?.roles) && u.roles.includes('admin')
  } catch {
    return false
  }
})

const retentionLabel = computed(() => `${retentionDays.value}天`)

function handleRetentionChange(val) {
  retentionDays.value = Number(val)
  pagination.value.page = 1
  fetchLogs()
}

async function fetchLogs() {
  loading.value = true
  try {
    const resp = await api.get('/api/admin/logs', {
      params: {
        page: pagination.value.page,
        page_size: pagination.value.pageSize,
        retention_days: retentionDays.value,
      },
    })
    items.value = resp.data.items || []
    pagination.value.total = resp.data.total ?? resp.data.items?.length ?? 0
    retentionDays.value = resp.data.retention_days ?? retentionDays.value
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载失败')
  } finally {
    loading.value = false
  }
}

function handlePageChange(page) {
  pagination.value.page = page
  fetchLogs()
}

function handleSizeChange(size) {
  pagination.value.pageSize = size
  pagination.value.page = 1
  fetchLogs()
}

onMounted(fetchLogs)
</script>

<template>
  <el-card class="main-card logs-fixed" shadow="hover">
    <template #header>
      <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap; justify-content: space-between">
        <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap">
          <div style="font-weight: 600">系统日志</div>
          <el-tag v-if="!isAdmin" type="warning">需要管理员权限</el-tag>
          <div class="retention-label-group">
            <span>仅保留近</span>
            <el-dropdown trigger="click" @command="handleRetentionChange">
              <span class="retention-chip" :class="{ 'is-loading': loading }">
                {{ retentionLabel }}
                <el-icon class="el-icon--right"><ArrowDown /></el-icon>
              </span>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item :disabled="retentionDays === 3" command="3">3天</el-dropdown-item>
                  <el-dropdown-item :disabled="retentionDays === 7" command="7">7天</el-dropdown-item>
                  <el-dropdown-item :disabled="retentionDays === 30" command="30">30天</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
        </div>
        <div style="display: flex; align-items: center; gap: 10px; flex-wrap: wrap">
          <el-button type="primary" :loading="loading" @click="fetchLogs">刷新</el-button>
        </div>
      </div>
    </template>

    <el-table :data="items" v-loading="loading" style="width: 100%">
      <el-table-column prop="created_at" label="时间" min-width="180" />
      <el-table-column prop="user_id" label="用户ID" width="100" />
      <el-table-column prop="method" label="方法" width="90" />
      <el-table-column prop="status_code" label="状态码" width="90" />
      <el-table-column prop="path" label="路径" min-width="240" />
      <el-table-column label="Meta" min-width="220">
        <template #default="{ row }">
          <el-text truncated>{{ JSON.stringify(row.meta || {}) }}</el-text>
        </template>
      </el-table-column>
    </el-table>
    <div class="table-pagination">
      <el-pagination
        v-model:current-page="pagination.page"
        v-model:page-size="pagination.pageSize"
        :total="pagination.total"
        :page-sizes="[12, 20, 50, 100]"
        layout="总数, sizes, prev, pager, next, jumper"
        background
        @current-change="handlePageChange"
        @size-change="handleSizeChange"
      />
    </div>
  </el-card>
</template>

<style scoped>
.retention-label-group {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  color: #606266;
  font-size: 14px;
}

.retention-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 5px 12px;
  border-radius: 999px;
  background: linear-gradient(135deg, #ecf5ff 0%, #d9ecff 100%);
  color: #2b6cb0;
  font-weight: 700;
  border: 1px solid rgba(64, 158, 255, 0.18);
  cursor: pointer;
  transition: all 0.2s ease;
  user-select: none;
  box-shadow: 0 4px 12px rgba(64, 158, 255, 0.12);
}

.retention-chip:hover {
  transform: translateY(-1px);
  box-shadow: 0 8px 18px rgba(64, 158, 255, 0.18);
}

.retention-chip.is-loading {
  opacity: 0.7;
  cursor: wait;
}

.table-pagination {
  margin-top: 20px;
}
</style>
