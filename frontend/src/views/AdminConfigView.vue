<script setup>
import { onMounted, ref } from 'vue'
import { ElMessage } from 'element-plus'
import { Setting, Refresh, Edit, Check, Close } from '@element-plus/icons-vue'
import api from '../api/client'

const loading = ref(false)
const items = ref([])

async function fetchConfigs() {
  loading.value = true
  try {
    const resp = await api.get('/api/admin/configs')
    items.value = resp.data.items.map(item => ({
      ...item,
      editing: false,
      editValue: item.value
    }))
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '加载配置失败')
  } finally {
    loading.value = false
  }
}

function startEdit(row) {
  row.editing = true
  row.editValue = row.value
}

function cancelEdit(row) {
  row.editing = false
}

async function saveEdit(row) {
  try {
    await api.post('/api/admin/configs', {
      key: row.key,
      value: row.editValue
    })
    row.value = row.editValue
    row.editing = false
    ElMessage.success('配置更新成功')
  } catch (e) {
    ElMessage.error(e?.response?.data?.error?.message || '更新失败')
  }
}

onMounted(fetchConfigs)
</script>

<template>
  <el-card class="main-card" shadow="hover">
    <template #header>
      <div style="display: flex; align-items: center; justify-content: space-between">
        <div style="display: flex; align-items: center; gap: 10px">
          <el-icon><Setting /></el-icon>
          <span style="font-weight: 600; font-size: 18px">系统配置管理</span>
        </div>
        <el-button type="primary" :icon="Refresh" :loading="loading" @click="fetchConfigs">刷新</el-button>
      </div>
    </template>

    <div class="config-container">
      <el-alert
        title="警告：修改系统配置可能会影响系统稳定性，请谨慎操作。"
        type="warning"
        show-icon
        :closable="false"
        style="margin-bottom: 20px"
      />

      <el-table :data="items" v-loading="loading" border stripe>
        <el-table-column prop="key" label="配置项" width="220">
          <template #default="{ row }">
            <code class="config-key">{{ row.key }}</code>
          </template>
        </el-table-column>
        <el-table-column prop="description" label="说明" min-width="200" />
        <el-table-column label="当前值" min-width="300">
          <template #default="{ row }">
            <div v-if="!row.editing" class="value-display">
              <span v-if="row.key.includes('KEY')" class="masked-value">********</span>
              <span v-else>{{ row.value }}</span>
            </div>
            <el-input
              v-else
              v-model="row.editValue"
              :placeholder="row.description"
              size="small"
              autofocus
            />
          </template>
        </el-table-column>
        <el-table-column label="操作" width="120" fixed="right">
          <template #default="{ row }">
            <template v-if="!row.editing">
              <el-button size="small" :icon="Edit" @click="startEdit(row)">编辑</el-button>
            </template>
            <template v-else>
              <div class="edit-actions">
                <el-button size="small" type="success" :icon="Check" circle @click="saveEdit(row)" />
                <el-button size="small" type="info" :icon="Close" circle @click="cancelEdit(row)" />
              </div>
            </template>
          </template>
        </el-table-column>
      </el-table>
    </div>
  </el-card>
</template>

<style scoped>
.config-container {
  padding: 10px 0;
}

.config-key {
  background-color: #f5f7fa;
  padding: 2px 6px;
  border-radius: 4px;
  color: #e6a23c;
  font-family: monospace;
}

.value-display {
  word-break: break-all;
  font-family: monospace;
}

.masked-value {
  color: #909399;
  letter-spacing: 2px;
}

.edit-actions {
  display: flex;
  gap: 5px;
}
</style>
